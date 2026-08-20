#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
聚影TV 采集站汇总过滤脚本（服务端版）
====================================
职责：把「ziyuanzu.com 每日检测结果 + 自建采集位置」的采集站汇总，
仿照 App 端 probeAliveStrict 逻辑（浏览接口返回真实片源 + 搜索关键词命中）
做服务端过滤，生成一份「干净采集站订阅」（TVBox sites 格式），
供 App 内置直接消费，替换 App 本地逐站检测流程。

用法：
    python build_sources.py [--output generated/sources.json]

输出：
    generated/sources.json  — {"updated_at":..., "sites":[...]} TVBox 兼容
    generated/sources.txt   — 纯 URL 列表（调试用）
"""

import json
import os
import re
import sys
import time
import urllib.request
import urllib.parse
import socket
import concurrent.futures as cf
from datetime import datetime

# ============ 配置 ============
ZYZ_URL = "https://www.ziyuanzu.com/download/ziyuanzu-cms-interfaces.json"
TIMEOUT = 10
MAX_WORKERS = 16
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

# 搜索验证词（与 App PROBE_TERMS 一致）
PROBE_TERMS = ["泰坦尼克号", "画江湖之天罡", "流浪地球", "你好李焕英", "长津湖", "满江红"]

# 成人/违规站黑名单（名称或域名包含即剔除）
NSFW_KEYWORDS = [
    "麻豆", "91md", "色", "av", "AV", "番号", "湿乐园", "福利", "搜a-v", "搜av",
    "x色", "X色", "色猫", "色仓库", "sex", "adult", "成人", "猎奇", "香蕉",
    "老色", "精品x", "精品X", "黑料", "白嫖", "美少女", "清水", "香系", "pgxdy",
]
# 域名黑名单（精确 host 后缀匹配）
NSFW_DOMAINS = [
    "mdzyapi.com", "xxavs.com", "fhapi9.com", "semaozy.net", "91md.me",
    "souavzyw.net", "kxgav.com", "msnii.com", "pgxdy.com", "xrbsp.com",
    "gdlsp.com", "hsckzy888.com", "heiliaozyapi.com", "jingpinx.com",
    "xxibaozyw.com", "xiangjiaozyw.com", "apilsbzy1.com", "lovedan.net",
    "vnzyz.com", "xxibaozyw.com",
]

# 疑似导航/占位/垃圾站域名
JUNK_DOMAINS = []

# 白名单：自建稳定采集站（App 现有 GITHUB_CONFIGS 中已验证的优质站，缺 ziyuanzu 覆盖时兜底）
# 格式: {name, api}
BUILTIN_SOURCES = [
    {"name": "非凡影视", "api": "http://api.ffzyapi.com/api.php/provide/vod"},
    {"name": "电影天堂采集", "api": "http://caiji.dyttzyapi.com/api.php/provide/vod"},
    {"name": "360影视", "api": "https://360zy.com/api.php/provide/vod"},
    {"name": "小虎影视", "api": "https://xgzyapi.com/api.php/provide/vod"},
    {"name": "暴风影视", "api": "https://bfzyapi.com/api.php/provide/vod"},
    {"name": "量子资源", "api": "https://lzzyapi.com/api.php/provide/vod"},
]

# 自建采集位置（App 端 GITHUB_CONFIGS 中的 type=1 采集源所在订阅，服务端汇总用）
UPSTREAM_SUBS = [
    # tvbw/Tvbox202408 zy.json：ziyuanzu 风格采集站聚合（鸭鸭/光速/天涯/牛牛/豆瓣等）
    "https://raw.githubusercontent.com/tvbw/Tvbox202408/main/zy.json",
    # 饭太硬 m.json：15 个 type1 采集源
    "https://raw.githubusercontent.com/liu673cn/box/master/m.json",
    # 资源猫内置快照（App res/raw/ziyuan_cat.json 同源）
    "https://raw.githubusercontent.com/CatMaven/ziyuan_cat/main/index.main.json",
]


def http_get(url, timeout=TIMEOUT):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "*/*"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8", "ignore")
    except Exception:
        return None


def host_of(url):
    try:
        u = urllib.parse.urlsplit(url if url.startswith("http") else "http://" + url)
        h = (u.hostname or "").lower()
        if h.startswith("www."):
            h = h[4:]
        return h
    except Exception:
        return ""


def key_of(api):
    """由 api 生成稳定 key：取 host 主域。"""
    h = host_of(api)
    if not h:
        return "src" + str(abs(hash(api)) % 100000)
    # 去掉公共后缀前的二级域名取主域，如 api.ffzyapi.com -> ffzyapi.com
    parts = h.split(".")
    if len(parts) >= 3:
        h = ".".join(parts[-2:])
    return h


def is_nsfw(name, api):
    hay = (name or "") + " " + (api or "")
    for kw in NSFW_KEYWORDS:
        if kw.lower() in hay.lower():
            return True
    h = host_of(api)
    for d in NSFW_DOMAINS:
        if h and (h == d or h.endswith("." + d)):
            return True
    return False


def strip_jsonc(s):
    out = []
    i, n = 0, len(s)
    instr = False
    esc = False
    while i < n:
        c = s[i]
        if instr:
            out.append(c)
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                instr = False
            i += 1
            continue
        if c == '"':
            instr = True
            out.append(c)
            i += 1
            continue
        if c == "/" and i + 1 < n and s[i + 1] == "/":
            while i < n and s[i] != "\n":
                i += 1
            continue
        if c == "/" and i + 1 < n and s[i + 1] == "*":
            i += 2
            while i + 1 < n and not (s[i] == "*" and s[i + 1] == "/"):
                i += 1
            i += 2
            continue
        out.append(c)
        i += 1
    return "".join(out)


def parse_sites_from_json(raw):
    """兼容多种订阅结构，提取 sites 列表（元素为 dict，含 name/api/type 等）。"""
    raw = strip_jsonc(raw or "")
    if not raw.strip():
        return []
    try:
        root = json.loads(raw)
    except Exception:
        return []
    sites = []
    if isinstance(root, list):
        # 裸数组：元素可能是 {"name","api"} 或 {"name","url"} 或 {"name","api","type"}
        for e in root:
            if isinstance(e, dict):
                api = e.get("api") or e.get("url") or ""
                if api:
                    sites.append({"name": e.get("name", ""), "api": api,
                                  "type": e.get("type", 1), "uptime": e.get("uptime", ""),
                                  "totalResources": e.get("totalResources", 0)})
        return sites
    if isinstance(root, dict):
        for key in ("sites", "spiders", "videos"):
            if isinstance(root.get(key), list):
                for e in root[key]:
                    if isinstance(e, dict):
                        api = e.get("api") or e.get("url") or ""
                        if api:
                            sites.append({"name": e.get("name", ""), "api": api,
                                          "type": e.get("type", 1), "uptime": e.get("uptime", ""),
                                          "totalResources": e.get("totalResources", 0)})
                if sites:
                    return sites
        # 直接是 {sites: [...]} 或 {config:{...}}
        if "config" in root and isinstance(root["config"], dict):
            c = root["config"]
            if isinstance(c.get("sites"), list):
                for e in c["sites"]:
                    if isinstance(e, dict):
                        api = e.get("api") or e.get("url") or ""
                        if api:
                            sites.append({"name": e.get("name", ""), "api": api,
                                          "type": e.get("type", 1)})
    return sites


def collect_ziyuanzu():
    """拉取 ziyuanzu.com 当日检测结果。"""
    raw = http_get(ZYZ_URL)
    if not raw:
        print("[WARN] ziyuanzu 拉取失败，跳过")
        return []
    try:
        arr = json.loads(raw)
        out = []
        for e in arr:
            if isinstance(e, dict) and e.get("api"):
                out.append({
                    "name": e.get("name", ""),
                    "api": e.get("api"),
                    "type": 1,
                    "uptime": e.get("uptime", ""),
                    "totalResources": int(e.get("totalResources", 0) or 0),
                    "src": "zyz",
                })
        return out
    except Exception as ex:
        print("[WARN] ziyuanzu 解析失败:", ex)
        return []


def collect_upstream():
    """拉取自建采集位置（GitHub 订阅）里的 type=1 采集站。"""
    out = []
    for url in UPSTREAM_SUBS:
        raw = None
        for cand in (url, "https://gh-proxy.com/" + url, "https://cdn.jsdelivr.net/gh/" + url.replace("https://raw.githubusercontent.com/", "").replace("/main/", "@main/").replace("/master/", "@master/")):
            raw = http_get(cand)
            if raw and raw.strip():
                break
        if not raw:
            print("[WARN] 上游拉取失败:", url)
            continue
        sites = parse_sites_from_json(raw)
        cnt = 0
        for s in sites:
            t = s.get("type", 1)
            if t is not None and str(t) not in ("1", "2"):
                continue  # 只收 type=1/2 HTTP 采集源；type=3 爬虫由 App 单独处理
            api = (s.get("api") or "").strip()
            if not api.startswith("http"):
                continue
            out.append({"name": s.get("name", ""), "api": api, "type": 1,
                        "uptime": "", "totalResources": 0, "src": "upstream"})
            cnt += 1
        print(f"[OK] 上游 {url.split('/')[-1]} 提取 {cnt} 个 type1 采集站")
    return out


def normalize_term(s):
    return re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]", "", s or "").lower()


def find_vod_array(obj):
    for key in ("list", "vod", "data"):
        v = obj.get(key)
        if isinstance(v, list):
            return v
        if isinstance(v, dict):
            for k2 in ("list", "vod"):
                if isinstance(v.get(k2), list):
                    return v[k2]
    return None


def probe_one(entry):
    """采集站存活判定：**只用搜索关键词方式**（用户要求，2026-08-21）。
    对每个 PROBE_TERM 发 ac=list&wd=<词> 搜索，任一词返回的片名真正包含该词即判活。
    去掉了原「先浏览 ac=list 判空列表」的前置判定——只保留搜索关键词命中一种方式。
    返回 (api, alive, reason)。"""
    api = entry["api"].strip()
    if not api.startswith("http"):
        return (api, False, "非http")
    sep = "&" if "?" in api else "?"
    # 只用搜索关键词命中判定（片名必须真正包含搜索词）
    for term in PROBE_TERMS:
        try:
            q = urllib.parse.quote(term)
            txt2 = http_get(api + sep + "ac=list&wd=" + q + "&pg=1", TIMEOUT)
            if not txt2:
                continue
            obj2 = json.loads(strip_jsonc(txt2))
            arr2 = find_vod_array(obj2)
            if not arr2:
                continue
            t = normalize_term(term)
            for v in arr2:
                if not isinstance(v, dict):
                    continue
                nm = v.get("vod_name") or v.get("name") or ""
                if t and t in normalize_term(nm):
                    return (api, True, f"搜索命中:{term}")
        except Exception:
            continue
    return (api, False, "搜索无命中")


def main():
    # 输出到脚本所在目录的 generated/ 子目录（脚本放仓库根目录：generated/sources.json）
    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "generated")
    os.makedirs(out_dir, exist_ok=True)

    print("=== 1. 汇总采集站 ===")
    zyz = collect_ziyuanzu()
    up = collect_upstream()
    print(f"ziyuanzu: {len(zyz)} 个，上游: {len(up)} 个")

    # 合并 + 去重（按 host 去重，保留 totalResources 大的）
    merged = {}
    for e in zyz + up:
        h = host_of(e["api"])
        if not h:
            continue
        if h in merged:
            if int(e.get("totalResources", 0) or 0) > int(merged[h].get("totalResources", 0) or 0):
                merged[h] = e
        else:
            merged[h] = e
    print(f"合并去重后: {len(merged)} 个")

    print("=== 2. 过滤（仅 NSFW 安全红线；存活判定统一交给搜索关键词探测） ===")
    # 用户要求（2026-08-21）：采集站过滤只用「搜索关键词」一种方式，其它判定都不要。
    # 故删除原「0 资源判死」前置过滤（那是 ziyuanzu totalResources 字段判定，属另一种方式）。
    # NSFW 属内容合规红线（非质量过滤），保留。
    clean = []
    for h, e in merged.items():
        if is_nsfw(e.get("name", ""), e["api"]):
            print(f"  [剔除-NSFW] {e.get('name')} {e['api']}")
            continue
        clean.append(e)
    print(f"过滤后: {len(clean)} 个")

    print("=== 3. 并发探测（仅搜索关键词命中判活） ===")
    alive = []
    with cf.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(probe_one, e): e for e in clean}
        for f in cf.as_completed(futs):
            e = futs[f]
            try:
                _, ok, reason = f.result()
            except Exception as ex:
                ok, reason = False, str(ex)
            if ok:
                alive.append(e)
                print(f"  [存活] {e.get('name')} {e['api']} ({reason})")
            else:
                print(f"  [剔除] {e.get('name')} {e['api']} ({reason})")
    print(f"存活: {len(alive)} / {len(clean)}")

    # 4) 生成干净订阅（TVBox sites 格式）
    sites = []
    for e in sorted(alive, key=lambda x: x.get("name", "")):
        api = e["api"].strip().rstrip("/")
        sites.append({
            "key": key_of(api),
            "name": (e.get("name") or key_of(api)).strip()[:12],
            "type": 1,
            "api": api,
            "searchable": 1,
            "quickSearch": 1,
            "filterable": 1,
        })

    out = {
        "version": 1,
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "count": len(sites),
        "sites": sites,
    }
    out_path = os.path.join(out_dir, "sources.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    with open(os.path.join(out_dir, "sources.txt"), "w", encoding="utf-8") as f:
        for s in sites:
            f.write(f"{s['name']}|{s['api']}\n")

    # 写采集统计（供邮件通知步骤读取）：采集前(合并去重后) / 过滤后(NSFW) / 存活(搜索命中)
    stats = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "zyz": len(zyz),
        "upstream": len(up),
        "merged": len(merged),
        "after_nsfw_filter": len(clean),
        "alive": len(alive),
        "published_sites": len(sites),
    }
    try:
        with open(os.path.join(out_dir, "stats_sources.json"), "w", encoding="utf-8") as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)
    except Exception as ex:
        print("[WARN] 写 stats_sources.json 失败:", ex)

    print(f"\n=== 完成: {out_path} ({len(sites)} 个干净采集站) ===")
    return 0 if sites else 1


if __name__ == "__main__":
    sys.exit(main())
