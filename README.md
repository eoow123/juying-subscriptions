# 聚影TV · 订阅仓库后端

为「订阅仓库」功能提供后端：每日从多个**实时更新的上游仓库**采集影视仓/TVBox 配置源、直播源、采集站，
统一清洗、去重、排序后，用 **RC4 + Base64** 加密成 blob，作为公开文件托管在 GitHub 仓库（经由 raw.githubusercontent.com / jsDelivr CDN 分发，零存储、零费用、无需绑定任何支付方式）。

App 端只需拉取这一个 blob → `Rc4Util.decrypt()` 解开 → 展示名称列表 → 用户点「添加」写入本地订阅。

---

## 架构

```
上游仓库(每日Action刷新)              本仓库(GitHub 公开仓库)
┌─────────────────────┐              ┌──────────────────────────┐
│ laoma2053/awesome-  │  raw.json   │  build.py                │
│   zhuiju-free       │ ──────────▶ │   抓取 → 解析 → 去重排序  │
│ hkbiang/TV (live)   │  result.txt │   → RC4+Base64           │
│ sdyby2006/TV (live) │ ──────────▶ │   产出 dist/repo.b64     │
│ to4kacc (采集站)    │  .json      │          │               │
│ jinenge/tvbox       │  tvbox.json │          ▼               │
│ 手动精选 curated     │  (静态)     │  git commit → 仓库公开文件 │
└─────────────────────┘              └──────────────────────────┘
                                              │ 公开分发
                                              ▼
                    raw.githubusercontent.com / cdn.jsdelivr.net/gh/...
                                              │
                                              ▼
                                    App: 拉取 → decrypt → 展示
```

**为什么不在 Cloudflare Worker 里构建？**
免费版 Worker 有 **10ms CPU/请求硬上限**，而「抓取 15+ GitHub 仓库 + 解析 JSON」远超该额度（会报 Error 1102）。
构建属于重活，放到 **GitHub Action（免费 Linux runner）** 跑；产物直接作为仓库公开文件提交回去，分发走 GitHub 原生 CDN（raw）或 jsDelivr，**整体成本 ≈ 0，且无需绑定任何支付方式**。

**为什么不做死链校验？**
GitHub Action 的数据中心 IP 会被很多上游当成代理/爬虫，返回假死（HTML 或 403），误判率极高。
本仓库只「从已校验的上游列表采集 + 手动精选」，实时性由上游各自的 Action 保证。

---

## 本地运行（验证用）

```bash
python build.py --out dist
```

- `dist/repo.json` ——  plaintext manifest（审计/调试）
- `dist/repo.b64`  —— 加密 blob（部署文件）
- 脚本内置 **RC4 往返自检**：解密回明文并断言与 manifest 一致，确保与 App 端 `Rc4Util.decrypt()` 完全兼容。

---

## 条目类型（type）

| type     | 含义                                  | App 端处理方式（延期实现）                     |
|----------|---------------------------------------|-----------------------------------------------|
| `config` | 影视仓/TVBox 配置 JSON 地址           | 直接加入 `Hawk SOURCE_SUBSCRIPTIONS`          |
| `live`   | 影视仓/TVBox 直播源 TXT 地址          | 加入直播订阅（TxtSubscribe / LivePlayActivity）|
| `spider` | 裸采集站 api（to4kacc 展开，已过滤成人站） | 包成单站最小配置后再加入；或单独处理           |

---

## 如何新增上游源

编辑 `build.py` 顶部的 `SOURCES` 列表，新增一项并指定 `parser`：

- `awesome_resources` —— `{resources:[{name,url,category}]}`，按 `category` 过滤
- `single_config` —— 整个 URL 即一个配置文件
- `single_live` —— 整个 URL 即一个直播源 TXT（需给 `name`）
- `omni_sites` —— `{sites:[{name/key, api, tags}]}`，展开为 spider 并过滤成人站
- `static_list` —— 直接写死 `items:[{name,url,type}]`（手动精选）

---

## 部署（纯 GitHub，零存储）

1. 把本目录作为独立**公开**仓库推到 GitHub（内含 `.github/workflows/build.yml`）。
2. 无需任何 Secrets 即可运行：每日 UTC 00:30（北京 08:30）Action 自动重建并把 `dist/repo.b64` 提交回仓库。
3. 可选：在仓库 `Settings → Secrets → Actions` 添加 `RC4_KEY`（留空则用 App 端默认密钥；用专用密钥时 App 端需同步新增常量）。
4. App 端拉取地址（任选，建议做多镜像 fallback）：
   - 主：`https://raw.githubusercontent.com/eoow123/juying-subscriptions/main/dist/repo.b64`
   - 镜像：`https://cdn.jsdelivr.net/gh/eoow123/juying-subscriptions@main/dist/repo.b64`
   - 镜像：`https://fastly.jsdelivr.net/gh/eoow123/juying-subscriptions@main/dist/repo.b64`

> 国内网络提示：`raw.githubusercontent.com` 在部分地区可能被限速/阻断；`jsDelivr` 通常有更好的国内可达性，App 端建议优先尝试 jsDelivr，失败再回退 raw。

---

## RC4 密钥说明

默认复用 App 端 `com.github.tvbox.osc.util.Rc4Util.KEY`
（`JUYING_APP_UPDATE_2026$Rc4#v1Key!`），因此 **App 端无需改任何解密代码即可直接解开本 blob**。

如需专用密钥：在 GitHub Secret `RC4_KEY` 设置，并同步在 App 端新增一个常量（例如
`SUBSCRIPTION_REPO_KEY`）让 `Rc4Util` 增加对应解密入口。加密算法本身与 App 端逐字节一致（标准 RC4，无丢弃前导字节）。

---

## 当前收录（最近一次本地构建）

- 总计 48 条：`config 20` / `live 2` / `spider 26`
- laoma2053 15 个配置端点（饭太硬/老刘备/小马/摸鱼儿/王二小…）
- hkbiang/TV、sdyby2006/TV 各 1 个直播源
- to4kacc 展开 26 个采集站（已过滤成人站）
- jinenge 内置源 + 9 个手动精选配置
