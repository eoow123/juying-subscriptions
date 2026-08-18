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
  - 直播源「后端预筛」（仅对标记 filter_live 的 single_live 源生效）：
      方案一（URL 规则，默认开启）：丢弃组播/内网代理地址（udp:// /rtp:// /…/udp/…
      /…/rtp/…，家庭普通网络基本放不出）与可配置黑名单域名。零请求、零误判。
      方案二（HTTP 探测，LIVE_PROBE=1 开启）：带浏览器 UA + Range 头 + 短超时并发
      探测，仅删「连接拒绝/DNS失败/404/410」等确定性死链；超时/403/5xx 保留（防误杀），
      结果写入 .probe_cache.json 跨次复用（TTL 7天）以降低 Actions IP 暴露。
      说明：原设计「不做死链校验」是因为 Actions 数据中心 IP 被上游当爬虫误判率高；
      方案二采用 fail-open + 增量缓存把误判风险压到最低，但默认仍关闭，先上稳的一。
  - 爬虫 jar 可达性筛查（JAR_FILTER，默认开启）：config 源配置内 spider 字段指向的
    .jar（常被伪装成 .jpg）若托管域名已死，App 端会报「所有代理均不可达」。构建期探测
    每个 config 源的 spider jar 是否可下载，确定性死链（404/410/连接拒绝/DNS失败）直接
    剔除，不进入 repo.json；超时/SSL/403 等不确定失败 fail-open 保留。设 JAR_FILTER=0
    关闭；JAR_FILTER_DRYRUN=1 只记录不剔除（用于验证）。
  - iptv-org 源：抓 channels.json + streams.json，按频道映射（跳过 feed/已停播/NSFW/
    明确失败状态），按国家分组生成 generated/iptv-org.txt；当前 API 无 status 字段时保留全部
    映射流，不自建探测（避免 Actions IP 误判）。
  - 所有「已筛」直播与 iptv-org 均落到 generated/ 由 CDN 分发；App 端订阅仓库点
    「添加」即取生成后的干净 URL，不再直连原始上游。
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
import concurrent.futures
import datetime
import json
import os
import re
import socket
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request

# ---------------------------------------------------------------------------
# RC4（与 App 端 com.github.tvbox.osc.util.Rc4Util 逐字节一致）
# ---------------------------------------------------------------------------
# App 端：key 按 (byte) key.charAt(i) & 0xFF 取字节；标准 RC4，无丢弃前导字节。
# 默认复用 App 推送更新密钥，使 blob 可被现有 Rc4Util.decrypt() 直接解开（前端零改动）。
DEFAULT_RC4_KEY = "JUYING_APP_UPDATE_2026$Rc4#v1Key!"
# 可通过环境变量覆盖（用户后续提供的专用密钥在此注入）
# 注意：os.environ.get 在「变量存在但为空串」时返回 "" 而非 fallback，
# 空串会导致 rc4() 里 len(key)=0 → ZeroDivisionError。故用 `or` 兜底到默认密钥
#（默认密钥与 App 端 Rc4Util.KEY 完全一致，保证 blob 可被 App 解密）。
RC4_KEY = (os.environ.get("RC4_KEY") or "").strip() or DEFAULT_RC4_KEY

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

# ---------------------------------------------------------------------------
# 直播源筛选（方案一：URL 规则；方案二：HTTP 探测）
# ---------------------------------------------------------------------------
# 生成的「已筛」直播文件托管在仓库 generated/ 目录，由 CDN 分发。
GENERATED_BASE_URL = (
    "https://cdn.jsdelivr.net/gh/eoow123/juying-subscriptions@main/generated/"
)

# rt_sync（GitHub Actions HTTP 实测）生成的单一国内直播聚合源，取代旧 hkbiang/sdyby2006/iptv-org 多个直播条目
CN_LIVE_TXT_NAME = "iptv_cn_filtered.txt"
CN_LIVE_TXT_URL = GENERATED_BASE_URL + CN_LIVE_TXT_NAME

# 探测用浏览器 UA：很多直播 CDN 对「非浏览器 UA」直接返回 403，必须用真实浏览器 UA 才探得出真假。
PROBE_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
PROBE_TIMEOUT = 8  # 秒（连接+读取总体上限，超时视为 unknown 保留，防误杀）
PROBE_WORKERS = 16  # 并发探测数
# 方案二默认关闭：GitHub Actions 数据中心 IP 极易被上游当爬虫/代理，超时/403 率高，
# 盲目探测会把大量「其实能放」的链接误判为死链。先上「方案一 URL 规则过滤」（零成本、零误判），
# 方案二设为可手动开启：在 Actions 中设环境变量 LIVE_PROBE=1 才启用。
LIVE_PROBE = os.environ.get("LIVE_PROBE") == "1"

# GitHub Token（仅用于给 GitHub 系域名(raw/api/github.io)的抓取加 Bearer 鉴权，
# 避免未登录限流、并让 Actions IP 更「像正常用户」）。绝不硬编码到代码，统一从环境变量读取。
GH_TOKEN = (os.environ.get("GH_TOKEN") or "").strip() or None

# 方案一：组播/内网代理地址——家庭普通网络基本放不出来，直接丢弃（零请求、零误判）。
# 形态：udp://... / rtp://... / http(s)://host/udp/239.x.x.x:port / .../rtp/...
_MULTICAST_RE = re.compile(r"(?i)(^(udp|rtp)://|(https?://[^/]+)?/(udp|rtp)(/|:))")
# 方案一：可配置域名黑名单（小写后缀匹配）。先留空，后续按需追加已知死域/鉴权失效域。
BLACKLIST_DOMAINS = [
    # "example-dead-domain.com",
]

# 方案二：增量探测缓存（落在 generated/ 随仓库提交，跨次运行复用，降低 Actions IP 暴露）。
PROBE_CACHE_FILE = os.path.join(GENERATED_DIR, ".probe_cache.json")
PROBE_CACHE_TTL_DAYS = 7


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
def _proxy_opener():
    """若环境设了 HTTPS_PROXY/HTTP_PROXY（含小写），则走代理抓取（本机可借此加速/绕过 iptv-org 直连限制）。
    未设置则返回默认 opener（直连）。Actions 环境通常不设代理，行为不变。"""
    proxies = {}
    for k in ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"):
        v = (os.environ.get(k) or "").strip()
        if v:
            key = "https" if k.lower().startswith("https") else "http"
            proxies[key] = v
    if proxies:
        return urllib.request.build_opener(urllib.request.ProxyHandler(proxies))
    return urllib.request.build_opener()


def fetch_text(url: str, timeout: int = None) -> str:
    timeout = timeout or FETCH_TIMEOUT
    last_err = None
    headers = {"User-Agent": FETCH_UA}
    host = urllib.parse.urlparse(url).netloc.lower()
    if GH_TOKEN and ("github.com" in host or "githubusercontent.com" in host
                     or "github.io" in host):
        headers["Authorization"] = f"Bearer {GH_TOKEN}"
    opener = _proxy_opener()
    for attempt in range(MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(url, headers=headers)
            with opener.open(req, timeout=timeout) as resp:
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
# 直播 txt 解析 / 过滤 / iptv-org 构建
# ---------------------------------------------------------------------------
# 频道元组：(group, name, url)
def parse_live_txt(text: str):
    """解析 #genre# txt 或 #EXTM3U，返回 [(group, name, url), ...]，保留顺序，去空。
    与 App 端 TxtSubscribe 解析规则对齐，保证生成文件 App 能直接消费。"""
    channels = []
    cur_group = "未分组"
    pending = None  # m3u 模式下暂存 (group, name)
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#EXTM3U"):
            continue
        if line.startswith("#EXTINF"):
            name = re.search(r",(.+?)$", line)
            name = name.group(1).strip() if name else "未命名"
            grp = re.search(r'group-title="(.*?)"', line)
            cur_group = grp.group(1).strip() if grp else cur_group
            pending = (cur_group, name)
            continue
        if pending is not None:
            # m3u 的 url 在 EXTINF 下一行
            channels.append((pending[0], pending[1], line))
            pending = None
            continue
        if "#genre#" in line:
            parts = line.split(",", 1)
            cur_group = parts[0].strip() or "未分组"
            continue
        # 普通行：name,url[#url2...]
        parts = line.split(",", 1)
        if len(parts) < 2:
            continue
        name = parts[0].strip()
        for u in parts[1].split("#"):
            u = u.strip()
            if u and (u.startswith("http") or u.startswith("rtsp") or u.startswith("rtmp")):
                channels.append((cur_group, name, u))
    return channels


def emit_live_txt(channels):
    """把 [(group, name, url)] 写回 #genre# txt（App TxtSubscribe.parseTxt 可解析）。"""
    lines = []
    last_group = None
    for grp, name, url in channels:
        if grp != last_group:
            lines.append(f"{grp},#genre#")
            last_group = grp
        lines.append(f"{name},{url}")
    return "\n".join(lines) + "\n"


def url_is_multicast(url: str) -> bool:
    return bool(_MULTICAST_RE.search(url or ""))


def url_blacklisted(url: str) -> bool:
    host = urllib.parse.urlparse(url).netloc.lower()
    for d in BLACKLIST_DOMAINS:
        if host == d or host.endswith("." + d):
            return True
    return False


def schema1_drop(url: str) -> bool:
    """方案一：确定性可丢规则（组播/黑名单）。返回 True 表示丢弃。"""
    return url_is_multicast(url) or url_blacklisted(url)


def probe_url(url: str) -> str:
    """方案二：探测单个 url。返回 'dead' / 'alive' / 'unknown'。
    仅 'dead'（连接拒绝 / DNS 失败 / 404 / 410）才删；超时/403/401/5xx 返回 unknown 保留，防误杀。"""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": PROBE_UA}, method="GET")
        req.add_header("Range", "bytes=0-1023")
        with urllib.request.urlopen(req, timeout=PROBE_TIMEOUT) as resp:
            code = resp.status
            if code in (404, 410):
                return "dead"
            return "alive"  # 200/206/3xx 都算活着；403/401/5xx 也保留（防反爬误杀）
    except urllib.error.HTTPError as e:
        if e.code in (404, 410):
            return "dead"
        return "unknown"
    except (urllib.error.URLError, socket.timeout, ConnectionError, OSError):
        # DNS 解析失败 / 连接被拒 / 网络不可达 → 确定性死链
        return "dead"
    except Exception:  # noqa: BLE001
        return "unknown"


def load_probe_cache() -> dict:
    try:
        with open(PROBE_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:  # noqa: BLE001
        return {}


def save_probe_cache(cache: dict):
    try:
        os.makedirs(os.path.dirname(PROBE_CACHE_FILE), exist_ok=True)
        with open(PROBE_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False)
    except Exception as e:  # noqa: BLE001
        print(f"  [WARN] write probe cache failed: {e}", file=sys.stderr)


def filter_live_channels(channels, enable_probe: bool):
    """方案一必做（丢组播/黑名单）；方案二可选（HTTP 探测删死链，带增量缓存）。
    返回 (filtered_channels, cache)。"""
    seen = set()
    kept = []
    for grp, name, url in channels:
        k = (grp, name, url)
        if k in seen:
            continue
        seen.add(k)
        if schema1_drop(url):
            continue
        kept.append((grp, name, url))

    if not enable_probe:
        return kept, load_probe_cache()

    cache = load_probe_cache()
    now = datetime.datetime.now(datetime.timezone.utc).timestamp()
    ttl = PROBE_CACHE_TTL_DAYS * 86400
    urls = [u for _, _, u in kept]
    results = {}

    def _do(u):
        c = cache.get(u)
        if c and (now - c.get("t", 0)) < ttl:
            return u, c.get("s", "unknown")
        return u, probe_url(u)

    with concurrent.futures.ThreadPoolExecutor(max_workers=PROBE_WORKERS) as ex:
        for u, s in ex.map(_do, urls):
            results[u] = {"s": s, "t": int(now)}

    out = []
    dropped = 0
    for grp, name, url in kept:
        if results.get(url, "unknown") == "dead":
            dropped += 1
            continue
        out.append((grp, name, url))
    cache.update(results)
    print(f"  [probe] 探测 {len(urls)} 条，删死链 {dropped} 条，保留 {len(out)} 条")
    return out, cache


def build_filtered_live(src: dict, text: str, out_dir: str):
    """single_live 源：抓 txt → 解析 → 方案一/二过滤 → 写 generated/<name>.txt → 返回 live 条目。"""
    channels = parse_live_txt(text)
    if not channels:
        return None, 0
    before = len(channels)
    filtered, cache = filter_live_channels(channels, enable_probe=LIVE_PROBE)
    if not filtered:
        # 全被过滤掉则不要发布空订阅（否则 App 拉到一个空列表）
        print(f"  [WARN] {src['id']} 过滤后为空，跳过发布（不产出空订阅）")
        return None, 0
    save_probe_cache(cache)
    os.makedirs(out_dir, exist_ok=True)
    fname = src.get("generated_name", src["id"] + ".txt")
    with open(os.path.join(out_dir, fname), "w", encoding="utf-8") as f:
        f.write(emit_live_txt(filtered))
    suffix = "（已筛·探测)" if LIVE_PROBE else "（已筛）"
    print(f"  [filter] {src['id']}: {before} → {len(filtered)} 条（丢 {before - len(filtered)}）")
    return {
        "name": (src.get("name") or _clean_name(src.get("url"))) + suffix,
        "url": GENERATED_BASE_URL + fname,
        "type": "live",
    }, len(filtered)


# iptv-org 国家码 → 中文名（常用），其余回退国家码
_ISO_CN = {
    "CN": "中国", "HK": "中国香港", "TW": "中国台湾", "MO": "中国澳门",
    "US": "美国", "GB": "英国", "JP": "日本", "KR": "韩国", "SG": "新加坡",
    "RU": "俄罗斯", "DE": "德国", "FR": "法国", "CA": "加拿大", "AU": "澳大利亚",
    "IN": "印度", "BR": "巴西", "IT": "意大利", "ES": "西班牙", "TH": "泰国",
    "MY": "马来西亚", "VN": "越南", "PH": "菲律宾", "ID": "印尼", "PK": "巴基斯坦",
    "TR": "土耳其", "SA": "沙特", "AE": "阿联酋", "EG": "埃及", "ZA": "南非",
    "MX": "墨西哥", "NL": "荷兰", "PT": "葡萄牙", "PL": "波兰", "UA": "乌克兰",
}

# iptv-org 只保留「中国」频道（大陆）。如需纳入港澳台，把 "HK"/"TW"/"MO" 加入即可。
CHINA_CODES = {"CN"}

# 中国频道按广电体系细分：央视台 / 卫视台 / 地方台（其余中国频道兜底归地方台）。
# 命名混合中英文（iptv-org 频道名常是 CCTV-1 / Hunan Satellite TV / 北京电视台 等），故双规则覆盖。
_CN_CAT_ORDER = {"央视台": 0, "卫视台": 1, "地方台": 2}


def _cn_category(name: str) -> str:
    n = (name or "").upper()
    if "CCTV" in n or "央视" in name or "中央" in name:
        return "央视台"
    if "卫视" in name or "SATELLITE" in n:
        return "卫视台"
    return "地方台"


def _fetch_first(urls, timeout: int = 120):
    """依次尝试多个镜像 URL，返回首个成功的内容；全部失败则抛出最后一个异常。"""
    last = None
    for u in urls:
        try:
            return fetch_text(u, timeout=timeout)
        except Exception as e:  # noqa: BLE001
            last = e
    raise last or RuntimeError("all urls failed")


def build_iptv_org(out_dir: str):
    """iptv-org/api：抓 channels.json + streams.json，按频道映射并过滤
    （跳过 feed/已停播/NSFW/明确失败状态），**只保留中国频道（央视台/卫视台/地方台）**，
    按分类分组生成 generated/iptv-org.txt（#genre#）。返回 (live 条目, 频道数)。
    注：不另行 HTTP 探测，避免 Actions IP 误判；本机若设了代理环境变量可加速抓取。"""
    urls_ch = [
        "https://iptv-org.github.io/api/channels.json",
        "https://raw.githubusercontent.com/iptv-org/api/master/channels.json",
    ]
    urls_st = [
        "https://iptv-org.github.io/api/streams.json",
        "https://raw.githubusercontent.com/iptv-org/api/master/streams.json",
    ]
    try:
        ch_text = _fetch_first(urls_ch, timeout=120)
        st_text = _fetch_first(urls_st, timeout=120)
        channels = json.loads(ch_text)
        streams = json.loads(st_text)
    except Exception as e:  # noqa: BLE001
        print(f"  [WARN] iptv-org fetch failed: {e}", file=sys.stderr)
        return None, 0

    by_id = {c.get("id"): c for c in channels}
    out = []
    seen = set()
    for s in streams:
        cid = s.get("channel")
        if not cid:
            continue  # feed 不带频道名，跳过（无法确定频道名/分组）
        url = normalize_url(s.get("url", ""))
        if not url or not url.startswith("http"):
            continue
        ch = by_id.get(cid)
        if not ch:
            continue
        if ch.get("closed"):
            continue  # 已停播频道，其流多为死链
        if ch.get("is_nsfw"):
            continue  # 过滤 NSFW
        # 仅跳过「明确失败」状态；无 status 字段（新 API 已移除）时全部保留。
        # 注：iptv-org 当前 streams.json 无 status 字段，故默认保留所有映射流。
        st = s.get("status")
        if st in ("offline", "timeout", "error", "dead"):
            continue
        # channels 字段兼容：旧 API 为 countries 数组，新 API 为 country 字符串
        country_field = ch.get("countries") or ch.get("country")
        if isinstance(country_field, list):
            code = country_field[0] if country_field else ""
        else:
            code = country_field or ""
        # ★ 仅保留中国频道（央视台/卫视台/地方台），其余国家全部丢弃
        if code not in CHINA_CODES:
            continue
        name = (ch.get("name") or cid).strip()
        if not name:
            continue
        group = _cn_category(name)  # 央视台 / 卫视台 / 地方台
        key = (group, name, url)
        if key in seen:
            continue
        seen.add(key)
        out.append((group, name, url))

    if not out:
        return None, 0
    # 按 央视台 → 卫视台 → 地方台 顺序排列，App 内分组清晰
    out.sort(key=lambda x: (_CN_CAT_ORDER.get(x[0], 9), x[1]))
    os.makedirs(out_dir, exist_ok=True)
    fname = "iptv-org.txt"
    with open(os.path.join(out_dir, fname), "w", encoding="utf-8") as f:
        f.write(emit_live_txt(out))
    entry = {
        "name": f"iptv-org 国内直播（{len(out)} 台）",
        "url": GENERATED_BASE_URL + fname,
        "type": "live",
    }
    return entry, len(out)



# ---------------------------------------------------------------------------
# 上游源定义（顺序即「从上到下」的展示顺序；laoma2053 为已校验骨干）
# ---------------------------------------------------------------------------
def make_cn_live_entry(txt_path: str):
    """读取 rt_sync 生成的国内直播聚合 txt（#genre# 格式），统计唯一频道(台)数，
    生成单个 live 订阅条目：国内直播(N台 YYYY-M-D)。
    rt_sync 的 CN_TV_SOURCES 已涵盖 iptv-org 中国频道，故本单源取代旧 hkbiang/sdyby2006/iptv-org 多个直播条目。
    txt 不存在/为空时返回 (None, 0)。"""
    try:
        with open(txt_path, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()
    except Exception as e:  # noqa: BLE001
        print(f"  [WARN] 读取 {txt_path} 失败: {e}", file=sys.stderr)
        return None, 0
    channels = set()
    for ln in lines:
        s = ln.strip()
        if not s or "#genre#" in s or s.startswith("#EXTM3U"):
            continue
        if ",http" in s or ",rtmp" in s or ",rtsp" in s:
            name = s.split(",", 1)[0].strip()
            if name:
                channels.add(name)
    n = len(channels)
    if n == 0:
        return None, 0
    # 北京时间（UTC+8）作为生成日期，与用户本地时区一致
    bj = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)
    date_str = f"{bj.year}-{bj.month}-{bj.day}"
    entry = {
        "name": f"国内直播({n}台 {date_str})",
        "url": CN_LIVE_TXT_URL,
        "type": "live",
    }
    return entry, n


SOURCES = [
    {
        "id": "laoma2053",
        "url": "https://raw.githubusercontent.com/laoma2053/awesome-zhuiju-free/main/resources/resources.json",
        "parser": "awesome_resources",
        "category": "tvbox_config",
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
# 爬虫 jar 可达性筛查（JAR_FILTER）
# ---------------------------------------------------------------------------
# 设备拉取 TVBox 配置后，配置内 spider 字段指向的 .jar（常被伪装成 .jpg 等扩展名）
# 若其托管域名已死/不可达，App 端会报「所有代理均不可达」。本步骤在构建期探测每个
# config 源的 spider jar 是否可下载，确定性死链直接剔除，使其不进入 repo.json，
# 从源头避免用户踩坑。
#
# 探测策略（fail-open，与 LIVE_PROBE 同源思路，避免 Actions IP 误判）：
#   - 候选地址：GitHub raw 类走「直连 + ghproxy/gh-proxy/gh.xxooo/gh.idayer 代理」
#     （设备端 downloadJarWithProxy 同款链路）；第三方域名仅直连。
#   - 判定：拿到 200/206 = 可达(保留)；404/410/连接拒绝/DNS失败 = 确定性死链(剔除)；
#     超时/SSL错误/403 = 不确定(保留，fail-open 防误杀)。
#   - 默认开启（JAR_FILTER=1）；设 JAR_FILTER=0 关闭；设 JAR_FILTER_DRYRUN=1
#     只记录不剔除（用于验证）。
# ---------------------------------------------------------------------------
GITHUB_PROXIES = [
    "https://ghproxy.net/",
    "https://gh-proxy.com/",
    "https://gh.xxooo.cf/",
    "https://gh.idayer.com/",
]
_JAR_PROBE_TIMEOUT = 15
_JAR_PROBE_CACHE = {}  # 同进程内去重，避免重复探测同一 URL


def _is_github_raw(url: str) -> bool:
    u = url.lower()
    return "raw.githubusercontent.com" in u or ("github.com" in u and "/raw/" in u)


def _jar_candidates(jar_url: str):
    if _is_github_raw(jar_url):
        return [jar_url] + [p + jar_url for p in GITHUB_PROXIES]
    return [jar_url]


def _extract_spider_urls(text: str):
    """从配置 JSON 中提取 spider 字段里的所有 http(s) URL（jar 常被伪装扩展名）。"""
    out = []
    try:
        data = json.loads(text)
    except Exception:  # noqa: BLE001
        return out
    if not isinstance(data, dict):
        return out

    def scan(sp):
        if not sp:
            return
        for tok in str(sp).split(","):
            tok = tok.strip()
            if tok.startswith("http://") or tok.startswith("https://"):
                out.append(tok)

    scan(data.get("spider"))
    for site in data.get("sites", []) or []:
        if isinstance(site, dict):
            scan(site.get("spider"))
    return out


def _probe_url(url: str):
    """返回 (状态, 备注)。状态: True=可达; False=确定性死链; None=不确定(fail-open)。"""
    if url in _JAR_PROBE_CACHE:
        return _JAR_PROBE_CACHE[url]
    candidates = _jar_candidates(url)
    definitive = False
    result = (None, "ambiguous")
    for c in candidates:
        try:
            req = urllib.request.Request(
                c, method="GET",
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Range": "bytes=0-1023",
                },
            )
            with urllib.request.urlopen(req, timeout=_JAR_PROBE_TIMEOUT) as resp:
                if resp.status in (200, 206):
                    result = (True, f"{c} -> {resp.status}")
                    break
                if resp.status in (404, 410):
                    definitive = True
        except urllib.error.HTTPError as e:
            if e.code in (404, 410):
                definitive = True
        except urllib.error.URLError as e:
            r = e.reason
            if isinstance(r, socket.timeout):
                pass
            elif isinstance(r, ConnectionError) or "Connection refused" in str(r) \
                    or "Name or service" in str(r) or "getaddrinfo" in str(r):
                definitive = True
        except (socket.timeout, ConnectionError, ssl.SSLError):
            pass  # 超时/SSL 不确定
        except Exception:  # noqa: BLE001
            pass
    if result[0] is not True and definitive:
        result = (False, "definitive failure (404/410/connection refused/DNS)")
    _JAR_PROBE_CACHE[url] = result
    return result


def filter_unreachable_jar(items, dry_run=False):
    out = []
    dropped = []
    for e in items:
        if e.get("type") != "config":
            out.append(e)
            continue
        url = e.get("url", "")
        if not url:
            out.append(e)
            continue
        try:
            text = fetch_text(url, timeout=30)
        except Exception as ex:  # noqa: BLE001
            print(f"  [JAR-FILTER] config fetch failed, keep: {e.get('name')}: {ex}",
                  file=sys.stderr)
            out.append(e)
            continue
        jar_urls = _extract_spider_urls(text)
        if not jar_urls:
            out.append(e)
            continue
        bad = []
        for ju in jar_urls:
            ok, note = _probe_url(ju)
            if ok is True:
                print(f"  [JAR-FILTER] OK   {e.get('name')} <- {ju} ({note})")
            elif ok is False:
                bad.append((ju, note))
                print(f"  [JAR-FILTER] BAD  {e.get('name')} <- {ju} ({note})", file=sys.stderr)
            else:
                print(f"  [JAR-FILTER] AMB  {e.get('name')} <- {ju} ({note}) (fail-open, keep)")
        if bad:
            if dry_run:
                print(f"  [JAR-FILTER][DRYRUN] would DROP {e.get('name')}: {len(bad)} dead jar(s)",
                      file=sys.stderr)
            else:
                dropped.append((e.get("name"), bad))
                print(f"  [JAR-FILTER] DROP {e.get('name')}: {len(bad)} unreachable spider jar(s)",
                      file=sys.stderr)
                continue
        out.append(e)
    return out, dropped


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

        # iptv-org：自行抓取并生成 generated/iptv-org.txt，产出单个 live 条目
        if sid == "iptv_org":
            try:
                entry, n = build_iptv_org(GENERATED_DIR)
            except Exception as e:  # noqa: BLE001
                print(f"  [WARN] source 'iptv_org' failed: {e}", file=sys.stderr)
                entry, n = None, 0
            per_source[sid] = n
            if entry:
                collected.append((sid, entry))
            continue

        # single_live + filter_live：抓取 txt → 方案一/二过滤 → 生成 generated 文件后发布
        if src.get("parser") == "single_live" and src.get("filter_live"):
            try:
                entry, n = build_filtered_live(src, text, GENERATED_DIR)
            except Exception as e:  # noqa: BLE001
                print(f"  [WARN] source '{sid}' failed: {e}", file=sys.stderr)
                entry, n = None, 0
            per_source[sid] = n
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

    # rt_sync 生成的单一国内直播源（取代旧 hkbiang/sdyby2006/iptv-org 多个直播条目）
    cn_live_entry, cn_live_n = make_cn_live_entry(os.path.join(GENERATED_DIR, CN_LIVE_TXT_NAME))
    if cn_live_entry:
        collected.insert(0, ("cn_live", cn_live_entry))
        per_source["cn_live"] = cn_live_n
    else:
        print("  [WARN] 未找到 iptv_cn_filtered.txt（rt_sync 未运行？），跳过国内直播单源", file=sys.stderr)

    # 去重（按 url 规范化键），保留首次出现顺序
    seen = set()
    items = []
    for sid, e in collected:
        k = dedup_key(e["url"])
        if k in seen:
            continue
        seen.add(k)
        items.append(e)

    # 爬虫 jar 可达性筛查（JAR_FILTER）：剔除 spider jar 确定性死链的 config 源，
    # 避免设备上「所有代理均不可达」。fail-open：超时/SSL/403 保留，仅 404/410/
    # 连接拒绝/DNS失败 才剔除。默认开启，JAR_FILTER=0 关闭，JAR_FILTER_DRYRUN=1 只记录不剔除。
    if os.environ.get("JAR_FILTER", "1") == "1":
        dry = os.environ.get("JAR_FILTER_DRYRUN", "0") == "1"
        items, dropped = filter_unreachable_jar(items, dry_run=dry)
        if dropped:
            names = ", ".join(n for n, _ in dropped)
            print(f"  [JAR-FILTER] 共剔除 {len(dropped)} 个源（确定性 jar 死链）：{names}",
                  file=sys.stderr)

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
    print(f"  直播筛选: 方案一(URL规则)=开"
          f"  方案二(HTTP探测)={'开(LIVE_PROBE=1)' if LIVE_PROBE else '关(默认)'}"
          f"  GitHub鉴权={'有' if GH_TOKEN else '无'}")
    print("  各源采集数:")
    for sid, n in per_source.items():
        print(f"    - {sid}: {n}")
    print(f"  明文: {plain_path}")
    print(f"  blob: {blob_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
