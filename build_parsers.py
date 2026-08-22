#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
聚影TV 解析端口采集脚本（服务端版，2026-08-22）
=============================================
职责：每日从「人工确认的种子站点 + Bing 国际版（经用户实测验证，国内版/百度/搜狗在服务器上被反爬拦截，不用）前 3 页」
发现候选站点，抓取页面提取解析端口（含名称），合并去重后按 HTTP 状态码过滤死链，
生成 generated/parsers.json（App 14 天缓存直接消费）与 generated/stats_parsers.json（汇报用）。

死链过滤：仅看 HTTP 状态码（2xx/3xx=活，其余=死），不解析响应内容，避免误杀好站。

输出：
    generated/parsers.json       — {"updated_at","count","ports":[{"name","url","source"}]}（仅活端口）
    generated/stats_parsers.json — 汇报数字（种子数/各引擎发现数/各站端口数/合并去重/去死链）
"""

import base64
import json
import re
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from os import path as ospath

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
BING_COOKIE = "SRCHHPGUSR=SRCHLANG=en|ensearch=1"
GOOGLE_COOKIE = "CONSENT=YES+cb.20210328-17-p0.en+FX+700; SOCS=CAISNQgQEitib3FfaWRlbnRpdHlmcm9udGVuZHVpc2VydmVyXzIwMjMwODI5LjA3X3AxGgJlbiACGgYIgLC_pwY"

# 15 个人工确认过的 VIP 解析聚合站（用户逐一点开核对）
SEED_SITES = [
    "https://www.toolbb.com/svipjiexi",
    "https://jiexi.6dk.cn/",
    "https://www.feiyudo.com/video/vip",
    "https://jxvip.z6.net.cn/",
    "https://www.quanminjiexi.com/",
    "https://www.tvff.cn/index.html",
    "https://www.pouyun.com/",
    "https://www.nuliya.top/vip/",
    "https://www.ityvip.xyz/",
    "https://www.t-d.ltd/",
    "https://zhiyifenxiang.com/vipjiexi/",
    "https://www.xxphp.cn/",
    "https://88lin.github.io/vip/index.html",
    "https://www.niudh.cn/tools/vip/",
    "https://www.tvff.cn/",
]

QUERY = "vip解析视频网站"
# Bing 用 first 偏移，Google 用 start 偏移；均为前三页（每页约 10 条）
BING_PAGES = [1, 11, 21]
GOOGLE_PAGES = [0, 10, 20]
MAX_SITES = 80            # 抓取站点上限，防爆量


def fetch(url: str, timeout: int = 15, extra_cookie: str = "", retries: int = 2):
    last = None
    for _ in range(retries):
        try:
            headers = {"User-Agent": UA, "Accept": "*/*"}
            if extra_cookie:
                headers["Cookie"] = extra_cookie
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                raw = r.read()
            enc = r.headers.get_content_charset() or "utf-8"
            try:
                return raw.decode(enc, errors="replace")
            except LookupError:
                return raw.decode("utf-8", errors="replace")
        except Exception as e:
            last = e
    raise last


def decode_bing_u(token: str):
    """Bing 结果链接 u=a1<base64> 解码出真实 URL。"""
    t = token[2:] if token.startswith("a1") else token
    for alpha in (t, t.replace("-", "+").replace("_", "/")):
        s = alpha + "=" * (-len(alpha) % 4)
        try:
            return base64.b64decode(s).decode("utf-8", errors="replace")
        except Exception:
            continue
    return None


_INTERNAL = r"bing\.com|google\.|gstatic|googleusercontent|youtube\.com|googleapis|microsoft|msn\.com|live\.com|aka\.ms|bingj\.com|mm\.bing\.net|w3\.org|schema\.org|duckduckgo\.com|baidu\.com|so\.com|sogou\.com"


def _is_internal(u: str) -> bool:
    return bool(re.search(_INTERNAL, u, re.I))


def bing_candidates(html: str):
    out = []
    for m in re.finditer(r"u=a1([A-Za-z0-9_\-+=]+)", html):
        real = decode_bing_u("a1" + m.group(1))
        if real and re.match(r"https?://", real) and not _is_internal(real):
            out.append(real)
    return out


def google_candidates(html: str):
    out = []
    # /url?q= 跳转形式
    for m in re.finditer(r"/url\?q=([^&]+)&?", html):
        try:
            real = urllib.parse.unquote(m.group(1))
        except Exception:
            continue
        if re.match(r"https?://", real) and not _is_internal(real):
            out.append(real)
    # 直接 https 链接
    for m in re.finditer(r'href="(https?://[^"]+)"', html):
        real = m.group(1)
        if not _is_internal(real):
            out.append(real)
    return out


def discover_via_bing():
    found = []
    for first in BING_PAGES:
        try:
            url = ("https://www.bing.com/search?q=" + urllib.parse.quote(QUERY) +
                   f"&ensearch=1&cc=US&first={first}")
            html = fetch(url, extra_cookie=BING_COOKIE)
            found.extend(bing_candidates(html))
        except Exception as e:
            print(f"  [!] Bing first={first} 失败: {e}")
    return _uniq(found)


def discover_via_google():
    # Google 国际版：GitHub Runner 实测网络可达但常返回 consent/captcha 拦截页（带 Cookie 也无法完全绕过），
    # 解析出 0 属正常；保留探测，若未来网络环境变化能出结果即自动纳入。
    found = []
    for start in GOOGLE_PAGES:
        try:
            url = ("https://www.google.com/search?q=" + urllib.parse.quote(QUERY) +
                   f"&start={start}&gl=us&hl=en&num=20&filter=0&pws=0")
            html = fetch(url, extra_cookie=GOOGLE_COOKIE)
            cands = google_candidates(html)
            found.extend(cands)
            if not cands:
                low = html[:3000].lower()
                if "unusual traffic" in low or "enablejs" in low or "consent.google.com" in low:
                    print(f"    [!] Google start={start} 疑似拦截页(consent/captcha/js)，本页无结果")
        except Exception as e:
            print(f"  [!] Google start={start} 失败: {e}")
    return _uniq(found)


def _uniq(items):
    seen, uniq = set(), []
    for u in items:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq


def resolve(u: str, base: str) -> str:
    """相对路径补全为绝对 URL。"""
    if re.match(r"https?://", u, re.I):
        return u
    if u.startswith("//"):
        return "https:" + u
    return urllib.parse.urljoin(base, u)


URLPARAM_MARKERS = r"url="
NOISE_HOSTS = ("googletagmanager.com", "wpa.qq.com", "gtag.", "google-analytics.com",
               "shturl.cc", "cnzz.", "umeng.", "baidu.com")
NOISE_EXT = (".js", ".css", ".png", ".jpg", ".jpeg", ".gif", ".woff", ".woff2", ".svg")


def is_noise(url: str) -> bool:
    u = url.lower()
    if "{" in u or "$" in u:          # ${item.value} 模板串
        return True
    if any(u.endswith(e) or "/templates/" in u or "/assets/" in u for e in NOISE_EXT):
        return True
    if any(h in u for h in NOISE_HOSTS):
        return True
    return False


def extract_ports(html: str, page_url: str):
    """返回 [(name, url)]。放宽三种来源，去重 key 忽略 url= 占位参数。"""
    cands = []  # (name, url, curated)  curated=来自下拉框/JS 对象（人工维护）
    base = page_url

    # 1) <option value="URL">NAME</option>（含裸域名与相对路径）
    for m in re.finditer(
        r"<option\b[^>]*\bvalue\s*=\s*[\"']([^\"']+)[\"'][^>]*>([^<]*)</option>",
        html, re.IGNORECASE,
    ):
        val = m.group(1).strip()
        if not val:
            continue
        url = resolve(val, base)
        name = m.group(2).strip() or url
        cands.append((name, url, True))

    # 2) JS 对象 {value:"URL", label:"NAME"}
    for blk in re.finditer(r"\{\s*([^}]*?)\s*\}", html):
        body = blk.group(1)
        vm = re.search(r"\b(?:value|url|src|api|playurl|link|address)\s*:\s*[\"']([^\"']+)[\"']", body, re.I)
        if not vm:
            continue
        url = resolve(vm.group(1).strip(), base)
        if not re.match(r"https?://", url, re.I):
            continue
        lm = re.search(r"\b(?:label|name|title|text|remark|desc)\s*:\s*[\"']([^\"']{1,40})[\"']", body, re.I)
        name = lm.group(1).strip() if lm else url
        cands.append((name, url, True))

    # 3) 任意含 ?url= 占位符的 URL 字符串（强特征，单独扫描）
    for m in re.finditer(
        r"(https?://[^\s\"'`<>]+?(?:[?&]" + URLPARAM_MARKERS + r"=))", html, re.I
    ):
        cands.append(("", m.group(1).strip(), False))

    # 去重 + 清洗
    best = {}
    order = []
    for name, url, curated in cands:
        if is_noise(url):
            continue
        try:
            p = urllib.parse.urlparse(url)
        except Exception:
            continue
        if not p.netloc:
            continue
        key = p.netloc.lower() + p.path.rstrip("/")
        if key not in best:
            best[key] = (name or url, url, curated)
            order.append(key)
        else:
            old_name, old_url, old_c = best[key]
            new_has = bool(re.search(r"[?&]url=", url, re.I))
            old_has = bool(re.search(r"[?&]url=", old_url, re.I))
            if new_has and not old_has:
                best[key] = (name or old_name or url, url, curated)
    return [(best[k][0], best[k][1]) for k in order]


def probe_base(port_url: str):
    """纯状态码探活：探测端口所在 host 根/路径是否可达。2xx/3xx=活，其余=死。不发内容判断。"""
    p = urllib.parse.urlparse(port_url)
    base = f"{p.scheme or 'https'}://{p.netloc}{p.path.rstrip('/') or '/'}"
    try:
        req = urllib.request.Request(base, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status in (200, 201, 202, 203, 204, 301, 302, 303, 307, 308)
    except Exception:
        return False


def main():
    out_dir = ospath.join(ospath.dirname(ospath.abspath(__file__)), "generated")
    import os
    os.makedirs(out_dir, exist_ok=True)

    print("[*] 阶段1a：Bing 国际版（前三页）发现候选站点 ...")
    bing_found = discover_via_bing()
    print(f"    Bing 发现 {len(bing_found)} 个候选")

    print("[*] 阶段1b：Google 国际版（前三页）发现候选站点 ...")
    google_found = discover_via_google()
    print(f"    Google 发现 {len(google_found)} 个候选")

    sites = list(SEED_SITES)
    for s in bing_found + google_found:
        if s not in sites:
            sites.append(s)
        if len(sites) >= MAX_SITES:
            break
    print(f"[*] 阶段2：抓取 {len(sites)} 个站点提取端口 ...\n")

    raw = {}
    for site in sites:
        tried = [site]
        try:
            tried.append("https://" + urllib.parse.urlparse(site).netloc + "/")
        except Exception:
            pass
        got = None
        used = None
        for url in tried:
            try:
                got = fetch(url)
                used = url
                break
            except Exception:
                continue
        if got:
            ps = extract_ports(got, used or site)
            if ps:
                raw[site] = ps
                print(f"    [抓到 {len(ps):2}] {site}")

    if not raw:
        print("    [!] 没有任何站点提取到端口")
        # 仍写出空结果，保证 stats 文件存在、构建不中断
        out_empty = {
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "count": 0,
            "ports": [],
        }
        stats_empty = {
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "seeds": len(SEED_SITES),
            "engine_bing_found": len(bing_found),
            "engine_google_found": len(google_found),
            "sites_scraped": 0,
            "per_site_counts": {},
            "merged_ports": 0,
            "alive": 0,
            "dead_removed": 0,
            "published_ports": 0,
        }
        with open(ospath.join(out_dir, "parsers.json"), "w", encoding="utf-8") as f:
            json.dump(out_empty, f, ensure_ascii=False, indent=2)
        with open(ospath.join(out_dir, "stats_parsers.json"), "w", encoding="utf-8") as f:
            json.dump(stats_empty, f, ensure_ascii=False, indent=2)
        print("[+] 已写出空 parsers.json / stats_parsers.json")
        return 1

    # 阶段3：合并去重（按 url 跨站去重，名字取首个非空，来源记录首个站点）
    merged = {}   # url -> {"name","source","sources":[...]}
    for site, ps in raw.items():
        for name, u in ps:
            if u not in merged:
                merged[u] = {"name": name or u, "source": site, "sources": [site]}
            else:
                if not merged[u]["name"] or merged[u]["name"] == u:
                    if name and name != u:
                        merged[u]["name"] = name
                if site not in merged[u]["sources"]:
                    merged[u]["sources"].append(site)
    merged_list = list(merged.items())  # [(url, meta)]

    print(f"\n[*] 阶段4：合并去重后 {len(merged_list)} 个端口，逐端口 HTTP 状态探活 ...")
    results = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = {ex.submit(probe_base, u): (u, meta) for u, meta in merged_list}
        for f in as_completed(futs):
            u, meta = futs[f]
            alive = f.result()
            results.append({"name": meta["name"], "url": u,
                            "alive": alive, "source": meta["source"]})

    alive_ports = [r for r in results if r["alive"]]
    alive_ports.sort(key=lambda x: (x["source"], x["name"]))

    out = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "count": len(alive_ports),
        # 仅发布活端口（死链已移除 = 最终发布）
        "ports": [{"name": r["name"], "url": r["url"], "source": r["source"]}
                  for r in alive_ports],
    }
    with open(ospath.join(out_dir, "parsers.json"), "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    stats = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "seeds": len(SEED_SITES),
        "engine_bing_found": len(bing_found),
        "engine_google_found": len(google_found),
        "sites_scraped": len(raw),
        "per_site_counts": {site: len(ps) for site, ps in raw.items()},
        "merged_ports": len(merged_list),                         # 合并去重后（探活前）
        "alive": len(alive_ports),
        "dead_removed": len(merged_list) - len(alive_ports),      # 去死链数量
        "published_ports": len(alive_ports),                      # = 最终发布数量
    }
    with open(ospath.join(out_dir, "stats_parsers.json"), "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    print(f"\n[+] 完成：合并去重 {stats['merged_ports']} 个 → 去死链 {stats['dead_removed']} 个 "
          f"→ 最终发布 {stats['published_ports']} 个端口")
    print(f"    -> generated/parsers.json , generated/stats_parsers.json")
    return 0 if alive_ports else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
