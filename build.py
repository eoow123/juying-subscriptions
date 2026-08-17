#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
聚影TV · 订阅仓库后端构建脚本（纯标准库，无第三方依赖）

职责：
  1. 从若干「实时更新的上游仓库」抓取数据；
  2. 按各自格式解析出 {name, url, type} 订阅条目；
  3. 去重 + 排序，组装成 manifest；
  4. 用 RC4 + Base64 加密（密钥与 App 端 Rc4Util.KEY 完全一致），
     产出可直接被 App 端 Rc4Util.decrypt() 解开的 blob。

输出：
  dist/repo.json   明文 manifest（仅供审计 / 调试）
  dist/repo.b64    加密后的 blob（部署到 CDN / R2 / Worker，App 拉取此文件）

设计要点：
  - 不做「死链校验」。GitHub Action 的数据中心 IP 会被很多上游当成爬虫/代理而
    返回假死（HTML 或 403），误判率极高。订阅仓库只「从已校验的上游列表采集 +
    手动精选」，实时性由上游各自的 Action 保证（hkbiang 每日、sdyby2006 每 12h、
    to4kacc 每日）。
  - type 字段：
      config  影视仓/TVBox 配置 JSON 地址（可直接加入 SOURCE_SUBSCRIPTIONS）
      live    影视仓/TVBox 直播源 TXT 地址（可加入直播订阅）

  说明：to4kacc 上游给的是 OmniBox 格式（站点 type=2 指向内置 spider），
  TVBox 无法直接消费。因此 build.py 会在构建时把其站点「去成人 + 转成
  TVBox 资源网直连配置（type 置空、直接用 api）」，落地为仓库 generated/
  目录下的单个 JSON，并以一个 config 条目发布，App 订阅后即获得整仓站点。
"""

import argparse
import base64
import datetime
import json
import os
import re
import sys
import urllib.parse
import urllib.request

# ---------------------------------------------------------------------------
# RC4（与 App 端 com.github.tvbox.osc.util.Rc4Util 逐字节一致）
# ---------------------------------------------------------------------------
# App 端：key 按 (byte) key.charAt(i) & 0xFF 取字节；标准 RC4，无丢弃前导字节。
# 默认复用 App 推送更新密钥，使 blob 可被现有 Rc4Util.decrypt() 直接解开（前端零改动）。
DEFAULT_RC4_KEY = "JUYING_APP_UPDATE_2026$Rc4#v1Key!"
# 可通过环境变量覆盖（用户后续提供的专用密钥在此注入）
RC4_KEY = os.environ.get("RC4_KEY", DEFAULT_RC4_KEY)

# to4kacc 转换后的 TVBox 多仓配置，托管在仓库 generated/ 目录，由 CDN 分发。
# App 端订阅仓库点「添加」时会把该 URL 写进 SOURCE_SUBSCRIPTIONS（name###url）。
TO4KACC_CONFIG_URL = (
    "https://cdn.jsdelivr.net/gh/eoow123/juying-subscriptions@main/"
    "generated/to4kacc-config.json"
)
# 仓库根目录下的 generated/ 文件夹（存放由 to4kacc 转换出的 TVBox 配置）
GENERATED_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "generated")

# 抓取 UA：用 okhttp 系 UA，规避部分上游对「浏览器 UA」返回 HTML 的反爬（摸鱼儿/王二小/饭太硬等）。
FETCH_UA = (
    "Mozilla/5.0 (Linux; Android 9; Pixel Build/PQ3A) "
    "okhttp/3.12.13 JuYingTV-SubscriptionBuilder"
)
FETCH_TIMEOUT = 25  # 秒
MAX_RETRIES = 2


def rc4(key: bytes, data: bytes) -> bytes:
    S = list(range(256))
    j = 0
    klen = len(key)
    for i in range(256):
        j = (j + S[i] + key[i % klen]) & 0xFF
        S[i], S[j] = S[j], S[i]
    out = bytearray(len(data))
    i = j = 0
    for k in range(len(data)):
        i = (i + 1) & 0xFF
        j = (j + S[i]) & 0xFF
        S[i], S[j] = S[j], S[i]
        out[k] = data[k] ^ S[(S[i] + S[j]) & 0xFF]
    return bytes(out)


def encrypt_manifest(manifest: dict, key: str) -> str:
    raw = json.dumps(manifest, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    cipher = rc4(key.encode("utf-8"), raw)
    return base64.b64encode(cipher).decode("ascii")


def decrypt_manifest(b64: str, key: str) -> dict:
    """自检用：与 encrypt 互逆，确认 App 端能解开。"""
    clean = re.sub(r"[^A-Za-z0-9+/=]", "", b64).replace("-", "+").replace("_", "/")
    plain = rc4(key.encode("utf-8"), base64.b64decode(clean))
    return json.loads(plain.decode("utf-8"))


# ---------------------------------------------------------------------------
# 抓取
# ---------------------------------------------------------------------------
def fetch_text(url: str) -> str:
    last_err = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": FETCH_UA})
            with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT) as resp:
                charset = resp.headers.get_content_charset() or "utf-8"
                return resp.read().decode(charset, errors="replace")
        except Exception as e:  # noqa: BLE001
            last_err = e
    raise RuntimeError(f"fetch failed after {MAX_RETRIES + 1} tries: {url} -> {last_err}")


# ---------------------------------------------------------------------------
# URL 规范化 / 去重键
# ---------------------------------------------------------------------------
def normalize_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    # 补全协议
    if url.startswith("//"):
        url = "https:" + url
    if not re.match(r"^https?://", url, re.I):
        url = "https://" + url
    return url


def dedup_key(url: str) -> str:
    p = urllib.parse.urlparse(url)
    netloc = p.netloc.lower()
    # 去掉默认端口，路径去尾斜杠
    host = netloc
    if ":" in host:
        h, port = host.split(":", 1)
        if port in ("80", "443"):
            host = h
    path = p.path.rstrip("/")
    return f"{p.scheme.lower()}://{host}{path}".lower()


# ---------------------------------------------------------------------------
# 解析器：每个返回 [{name, url, type}, ...]
# ---------------------------------------------------------------------------
def _clean_name(name: str) -> str:
    name = (name or "").strip()
    # 去掉影视仓站点常见的装饰前缀，如 "🎬-爱奇艺-" -> "爱奇艺"
    name = re.sub(r"^[\U0001F300-\U0001FAFF\u2600-\u27BF\s\-]+", "", name)
    name = name.strip(" -")
    return name


def parse_awesome_resources(source: dict, text: str):
    data = json.loads(text)
    cat = source.get("category", "tvbox_config")
    out = []
    for item in data.get("resources", []):
        if item.get("category") != cat:
            continue
        name = _clean_name(item.get("name", ""))
        url = normalize_url(item.get("url", ""))
        if not name or not url:
            continue
        out.append({"name": name, "url": url, "type": "config"})
    return out


def parse_single_live(source: dict, text: str):
    url = normalize_url(source["url"])
    name = _clean_name(source.get("name", url))
    if not url:
        return []
    return [{"name": name, "url": url, "type": "live"}]


def parse_single_config(source: dict, text: str):
    url = normalize_url(source["url"])
    name = _clean_name(source.get("name", url))
    if not url:
        return []
    return [{"name": name, "url": url, "type": "config"}]


def make_to4kacc_config(text: str, out_dir: str):
    """to4kacc 上游是 OmniBox 格式（sites[] 每项含 api + type=2 内置 spider）。
    TVBox 无法直接消费 type=2，因此这里把每个非成人站点转成 TVBox 可直接
    调用的「资源网直连」站点（type 置空、直接用 api），汇总为一个 TVBox 配置，
    写入 out_dir/to4kacc-config.json。返回 (单个 config 条目, 内部站点数)，
    失败/无有效站点时返回 (None, 0)。"""
    try:
        data = json.loads(text)
    except Exception:  # noqa: BLE001
        return None, 0
    out_sites = []
    for site in data.get("sites", []):
        tags = site.get("tags", []) or []
        if any("成人" in str(t) for t in tags):
            continue
        if "🔞" in (site.get("key", "") + site.get("name", "")):
            continue
        api = normalize_url(site.get("api", ""))
        if not api:
            continue
        name = _clean_name(site.get("name") or site.get("key", ""))
        if not name:
            continue
        out_sites.append({
            "key": site.get("id") or name,
            "name": name,
            "api": api,
            "type": "",
        })
    if not out_sites:
        return None, 0
    cfg = {"spider": "", "sites": out_sites, "pass": True}
    try:
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, "to4kacc-config.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
    except Exception as e:  # noqa: BLE001
        print(f"  [WARN] write to4kacc config failed: {e}", file=sys.stderr)
        return None, 0
    entry = {
        "name": f"to4kacc 多仓（{len(out_sites)} 站·资源网直连）",
        "url": TO4KACC_CONFIG_URL,
        "type": "config",
    }
    return entry, len(out_sites)


def parse_static_list(source: dict, text: str):
    out = []
    for item in source.get("items", []):
        name = _clean_name(item.get("name", ""))
        url = normalize_url(item.get("url", ""))
        t = item.get("type", "config")
        if not name or not url:
            continue
        out.append({"name": name, "url": url, "type": t})
    return out


PARSERS = {
    "awesome_resources": parse_awesome_resources,
    "single_live": parse_single_live,
    "single_config": parse_single_config,
    "static_list": parse_static_list,
}


# ---------------------------------------------------------------------------
# 上游源定义（顺序即「从上到下」的展示顺序；laoma2053 为已校验骨干）
# ---------------------------------------------------------------------------
SOURCES = [
    {
        "id": "laoma2053",
        "url": "https://raw.githubusercontent.com/laoma2053/awesome-zhuiju-free/main/resources/resources.json",
        "parser": "awesome_resources",
        "category": "tvbox_config",
    },
    {
        "id": "hkbiang_live",
        "name": "hkbiang/TV 直播源（每日校验）",
        "url": "https://raw.githubusercontent.com/hkbiang/TV/master/result.txt",
        "parser": "single_live",
    },
    {
        "id": "sdyby2006_live",
        "name": "sdyby2006/TV 直播源（每 12h 校验）",
        "url": "https://raw.githubusercontent.com/sdyby2006/TV/master/result.txt",
        "parser": "single_live",
    },
    {
        "id": "to4kacc",
        "name": "to4kacc 多仓（资源网直连）",
        "url": "https://raw.githubusercontent.com/to4kacc/LunaTV-config-to-OmniBox-config/main/converted_data.json",
    },
    {
        "id": "jinenge",
        "name": "jinenge 内置源（自动更新）",
        "url": "https://raw.githubusercontent.com/jinenge/tvbox/master/tvbox.json",
        "parser": "single_config",
    },
    {
        "id": "curated",
        "parser": "static_list",
        "items": [
            {"name": "dxawi 0", "url": "https://dxawi.github.io/0/0.json", "type": "config"},
            {"name": "liu673cn 盒子", "url": "https://liu673cn.github.io/box/m.json", "type": "config"},
            {"name": "PyramidStore", "url": "https://raw.githubusercontent.com/UndCover/PyramidStore/main/py.json", "type": "config"},
            {"name": "home.jundie top98", "url": "http://home.jundie.top:81/top98.json", "type": "config"},
            {"name": "肥猫（单仓）", "url": "http://肥猫.net/", "type": "config"},
            {"name": "小盒子 4K", "url": "http://xhztv.top/4k.json", "type": "config"},
            {"name": "小盒子多仓", "url": "http://xhztv.top/dc", "type": "config"},
            {"name": "拾光多仓", "url": "http://xmbjm.fh4u.org/dc.txt", "type": "config"},
            {"name": "挺好分享多仓", "url": "http://ztha.top/TVBox/GYCK.json", "type": "config"},
        ],
    },
]


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
def build() -> dict:
    collected = []  # (source_id, entry)
    per_source = {}
    for src in SOURCES:
        sid = src["id"]
        try:
            text = fetch_text(src["url"]) if src.get("url") else ""
        except Exception as e:  # noqa: BLE001
            print(f"  [WARN] source '{sid}' fetch failed: {e}", file=sys.stderr)
            text = ""
        # to4kacc 不走通用 parser，而是转成 TVBox 多仓配置后产出单个 config 条目
        if sid == "to4kacc":
            try:
                entry, n_sites = make_to4kacc_config(text, GENERATED_DIR)
            except Exception as e:  # noqa: BLE001
                print(f"  [WARN] source 'to4kacc' failed: {e}", file=sys.stderr)
                entry, n_sites = None, 0
            per_source[sid] = n_sites
            if entry:
                collected.append((sid, entry))
            continue
        try:
            entries = PARSERS[src["parser"]](src, text)
        except Exception as e:  # noqa: BLE001
            print(f"  [WARN] source '{sid}' failed: {e}", file=sys.stderr)
            entries = []
        per_source[sid] = len(entries)
        for e in entries:
            collected.append((sid, e))

    # 去重（按 url 规范化键），保留首次出现顺序
    seen = set()
    items = []
    for sid, e in collected:
        k = dedup_key(e["url"])
        if k in seen:
            continue
        seen.add(k)
        items.append(e)

    # 统计 type
    type_count = {}
    for it in items:
        type_count[it["type"]] = type_count.get(it["type"], 0) + 1

    manifest = {
        "version": 1,
        "updated_at": datetime.datetime.now(datetime.timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "count": len(items),
        "items": items,
    }
    return manifest, per_source, type_count


def main():
    ap = argparse.ArgumentParser(description="Build JuYingTV subscription repository blob")
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "dist"),
                    help="output directory (default: ./dist)")
    ap.add_argument("--no-verify", action="store_true", help="skip RC4 round-trip self-check")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    manifest, per_source, type_count = build()

    # 明文
    plain_path = os.path.join(args.out, "repo.json")
    with open(plain_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    # 加密 blob
    blob = encrypt_manifest(manifest, RC4_KEY)
    blob_path = os.path.join(args.out, "repo.b64")
    with open(blob_path, "w", encoding="utf-8") as f:
        f.write(blob)

    # 自检：解密回明文，确认与 App 端 Rc4Util.decrypt 完全兼容
    if not args.no_verify:
        restored = decrypt_manifest(blob, RC4_KEY)
        assert restored == manifest, "RC4 round-trip mismatch!"
        print("  [OK] RC4 round-trip verified (blob <-> manifest)")

    # 报告
    print("=" * 60)
    print(f"  订阅仓库构建完成")
    print(f"  总条目: {manifest['count']}  (type: {type_count})")
    print(f"  RC4 密钥: {'默认(与App一致)' if RC4_KEY == DEFAULT_RC4_KEY else '环境变量覆盖'}")
    print("  各源采集数:")
    for sid, n in per_source.items():
        print(f"    - {sid}: {n}")
    print(f"  明文: {plain_path}")
    print(f"  blob: {blob_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
