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

# 成人/违规站黑名单：**已废弃**（2026-08-21 用户明确要求：只用搜索关键词判活，
# 探测词均为正经电影，不良站搜不到 → 自然判死，无需域名/名称黑名单；成人分类过滤挪到 App 端）。
# 保留空列表 + is_nsfw 定义仅为兼容旧引用，实际 main() 已不调用。
NSFW_KEYWORDS = []
# 域名黑名单（已废弃，清空）
NSFW_DOMAINS = []

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


# 我们自己发布的 repo.json（dist/repo.json）：type=config 条目里嵌着很多子配置 URL，
# 每份子配置的 sites 里往往含 type=1 采集站。用户要求（2026-08-21）：把这些"经我们筛选后
# 保存下来的 config 里的 type=1 采集站"提取出来，作为采集站来源，再统一走搜索判活+汇总。
REPO_JSON_URLS = [
    "https://repo.eoow.top/dist/repo.json",
    "https://cdn.jsdelivr.net/gh/eoow123/juying-subscriptions@main/dist/repo.json",
]


def _extract_type1_from_config_url(cfg_url, depth=0):
    """下载一份 TVBox config，提取其中 type=1 采集站（api 以 http 开头）。
    支持 multi-repo（含 urls[] 子仓库列表）一层递归展开。返回 [{name, api}]。"""
    out = []
    if not cfg_url or not cfg_url.startswith("http"):
        return out
    raw = None
    # 中文域名/被墙 raw 一律尝试镜像回退
    cands = [cfg_url]
    if "raw.githubusercontent.com" in cfg_url:
        cands.append("https://cdn.jsdelivr.net/gh/" + cfg_url.replace(
            "https://raw.githubusercontent.com/", "").replace("/main/", "@main/").replace("/master/", "@master/"))
        cands.append("https://gh-proxy.com/" + cfg_url)
    for c in cands:
        raw = http_get(c, TIMEOUT)
        if raw and raw.strip():
            break
    if not raw:
        return out
    body = strip_jsonc(raw)
    try:
        root = json.loads(body)
    except Exception:
        return out
    # multi-repo：{"urls":[{"url":...},...]} 一层展开（避免无限递归，depth<=1）
    if isinstance(root, dict) and isinstance(root.get("urls"), list) and depth < 1:
        for u in root["urls"]:
            sub = (u.get("url") if isinstance(u, dict) else u) or ""
            out.extend(_extract_type1_from_config_url(sub, depth + 1))
        return out
    for s in parse_sites_from_json(raw):
        t = s.get("type", 1)
        if t is not None and str(t) not in ("1",):
            continue  # 采集站只要 type=1（AppleCMS JSON）；type=0/3/4 不是标准采集接口
        api = (s.get("api") or "").strip()
        if not api.startswith("http"):
            continue
        out.append({"name": s.get("name", ""), "api": api})
    return out


def collect_from_repo_configs():
    """从我们自己发布的 repo.json 里的 config 条目，逐个下载子配置，提取 type=1 采集站。"""
    out = []
    repo = None
    for u in REPO_JSON_URLS:
        repo = http_get(u, TIMEOUT)
        if repo and repo.strip():
            break
    if not repo:
        print("[WARN] repo.json 拉取失败，跳过 config 内 type=1 提取")
        return out
    try:
        obj = json.loads(strip_jsonc(repo))
    except Exception as ex:
        print("[WARN] repo.json 解析失败:", ex)
        return out
    items = obj.get("items", []) if isinstance(obj, dict) else []
    cfg_urls = [it.get("url") for it in items
                if isinstance(it, dict) and it.get("type") == "config" and it.get("url")]
    print(f"[repo.json] config 条目 {len(cfg_urls)} 个，逐个提取 type=1 采集站…")
    seen = set()
    for cu in cfg_urls:
        try:
            got = _extract_type1_from_config_url(cu)
        except Exception:
            got = []
        n = 0
        for g_ in got:
            api = g_["api"].strip()
            h = host_of(api)
            if not h or h in seen:
                continue
            seen.add(h)
            out.append({"name": g_.get("name", ""), "api": api, "type": 1,
                        "uptime": "", "totalResources": 0, "src": "repo_config"})
            n += 1
        tag = cu if len(cu) < 60 else cu[:57] + "..."
        print(f"  [config] {tag} → {n} 个 type1")
    print(f"[repo.json] 合计从 config 提取 {len(out)} 个 type1 采集站")
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


def _snapshot_reconcile(alive, out_dir, kind="sources"):
    """快照对照 + 差异补筛（用户要求 2026-08-21）。

    语义："结果永远 >= 上次成功快照"——具体指：不因上游某次抖动/整站临时关闭而误丢
    仍然活着的采集站。做法：
      1. 读取上次快照 snapshot_<kind>.json（首次运行则无，直接以本次为准）。
      2. 找出"上次有、本次不在 alive 里"的站（diff）。
      3. 对 diff 里的每个站重新走一轮 probe_one 搜索判活；仍命中的补回 alive。
      4. 把补齐后的 alive 覆盖写回快照，供下次对照。
    真正死掉的站（重探仍无命中）不会补回，自然淘汰。
    alive 元素为 dict（含 name/api）。返回补齐后的 alive。"""
    snap_path = os.path.join(out_dir, f"snapshot_{kind}.json")
    alive_hosts = {host_of(e["api"]) for e in alive if e.get("api")}

    prev_sites = []
    if os.path.exists(snap_path):
        try:
            with open(snap_path, "r", encoding="utf-8") as f:
                prev = json.load(f)
            prev_sites = prev.get("sites", []) if isinstance(prev, dict) else []
        except Exception as ex:
            print(f"[快照] 读取 {snap_path} 失败（忽略）: {ex}")

    # 找差异：上次有、本次 alive 里没有的站
    missing = []
    for s in prev_sites:
        api = (s.get("api") or "").strip()
        h = host_of(api)
        if api.startswith("http") and h and h not in alive_hosts:
            missing.append({"name": s.get("name", ""), "api": api})

    recovered = 0
    if missing:
        print(f"[快照] 上次 {len(prev_sites)} 站，本次活 {len(alive)} 站；差异 {len(missing)} 站→重探补筛")
        with cf.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futs = {ex.submit(probe_one, m): m for m in missing}
            for f in cf.as_completed(futs):
                m = futs[f]
                try:
                    _, ok, reason = f.result()
                except Exception:
                    ok, reason = False, "异常"
                if ok:
                    alive.append({"name": m.get("name", ""), "api": m["api"], "type": 1,
                                  "uptime": "", "totalResources": 0, "src": "snapshot"})
                    alive_hosts.add(host_of(m["api"]))
                    recovered += 1
                    print(f"  [快照补回] {m.get('name')} {m['api']} ({reason})")
                else:
                    print(f"  [快照淘汰] {m.get('name')} {m['api']} ({reason})")
        print(f"[快照] 补回 {recovered} 站，合计 {len(alive)} 站")
    else:
        print(f"[快照] 无差异或首次运行（上次 {len(prev_sites)} 站）")

    # 覆盖写回本次快照（补齐后的完整活站列表）
    try:
        snap = {
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "count": len(alive),
            "sites": [{"name": e.get("name", ""), "api": e["api"]} for e in alive],
        }
        with open(snap_path, "w", encoding="utf-8") as f:
            json.dump(snap, f, ensure_ascii=False, indent=2)
    except Exception as ex:
        print(f"[快照] 写回 {snap_path} 失败: {ex}")
    return alive


def main():
    # 输出到脚本所在目录的 generated/ 子目录（脚本放仓库根目录：generated/sources.json）
    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "generated")
    os.makedirs(out_dir, exist_ok=True)

    print("=== 1. 汇总采集站 ===")
    zyz = collect_ziyuanzu()
    up = collect_upstream()
    repo_cfg = collect_from_repo_configs()  # 从我们发布的 repo.json 里 config 提取 type=1 采集站
    print(f"ziyuanzu: {len(zyz)} 个，上游: {len(up)} 个，repo.json config: {len(repo_cfg)} 个")

    # 合并（用户要求 2026-08-21：中间不去重，统一放到最后探测后再去重）。
    # 原因：探测前按 host 去重会用 totalResources 规则挑选——可能把"活站"因 totalResources
    #      小而删掉、反留"死站"（totalResources 大但实际搜不到），导致探测后活站变少。
    #      放到探测后（剩下的全是活站）再去重，才能保证留下来的一定是活的。
    #      域名不同的站（如 ffzy1~5.tv / api.ffzyapi.com）host 不同，去重不受影响，全部保留。
    clean = [e for e in (zyz + up + repo_cfg) if host_of(e.get("api", ""))]
    print(f"合并候选（未去重）: {len(clean)} 个")

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

    # ============ 统一去重（用户要求 2026-08-21：探测后再按 host 去重） ============
    # 探测后剩下的全是活站，此时按 host 去重能保证留下的一定是活的（探测前去重可能误删活站）。
    # 域名不同的站全部保留（如 ffzy1~5.tv / api.ffzyapi.com host 不同 → 都留）。
    # 同 host 重复时保留 totalResources 更大的。
    dedup = {}
    for e in alive:
        h = host_of(e.get("api", ""))
        if not h:
            continue
        if h in dedup:
            if int(e.get("totalResources", 0) or 0) > int(dedup[h].get("totalResources", 0) or 0):
                dedup[h] = e
        else:
            dedup[h] = e
    if len(dedup) != len(alive):
        print(f"探测后按 host 去重: {len(alive)} → {len(dedup)} 个（删除 {len(alive) - len(dedup)} 个重复 host）")
    alive = list(dedup.values())

    # ============ 快照对照 + 差异补筛（用户要求 2026-08-21，任务4）============
    # 目标：结果永远 >= 上一次成功快照。若本次上游整站关闭导致某活站消失，
    #       用上次快照里的该站重新走一轮搜索判活，活的补回本次结果。
    #   - 快照存 generated/snapshot_sources.json（{updated_at, sites:[{name,api}]}）。
    #   - 只补"上次有、本次没有、且现在仍搜索命中"的站；死站不会被补回（保持只增不减的语义
    #     指的是"不因上游一次抖动而丢失仍然活着的站"，真死了的自然淘汰）。
    alive = _snapshot_reconcile(alive, out_dir, kind="sources")

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
        "merged": len(clean),  # 合并候选(未去重, 用户要求探测后才去重)
        "after_nsfw_filter": len(clean),  # 已不做黑名单过滤，等于候选数（保留字段兼容邮件/状态页）
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
