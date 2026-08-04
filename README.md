# EEW Relay

面向 iOS Bark 的 Wolfx 地震预警订阅服务。它提供一个公开网页，用户填写 Bark Key、经纬度和推送阈值；后端常驻监听 Wolfx EEW WebSocket，收到首报后按每个订阅者位置估算 S 波到达时间和本地烈度，并尽快通过 Bark 推送。

参考项目形态：<https://github.com/noctiro/earthquake-alert>、<https://eew.noctiro.moe/>

本仓库是可独立部署的开源版本。所有通过订阅接口成功提交的用户都会保存为正式订阅；首次订阅仍会发送一条测试通知，用于验证 Bark 推送链路。

## 文档导航

- [开源版本部署指南](docs/OPEN_SOURCE_DEPLOYMENT.md)
- [分支与发布工作流](docs/BRANCH_AND_RELEASE_WORKFLOW.md)
- [变更检查清单](docs/CHANGE_CHECKLIST.md)
- [参与贡献](CONTRIBUTING.md)

凡是修改用户行为、API、配置、依赖或部署流程，都必须在同一变更中维护对应文档和测试。

## 特性

- 公开订阅页面：`/`
- 订阅 API：`POST /api/subscribe`
- 新用户首次验证成功后直接保存为正式订阅
- 用户管理页面：`/manage`，Bark Key 只放在 URL fragment 或请求头中
- 用户管理 API：`GET /api/subscription`、`DELETE /api/unsubscribe`
- 管理员后台：`/admin`，支持订阅管理、通知审计、健康历史时间轴、单用户测试和系统自检
- 统计 API：`GET /api/stats`
- 健康检查：`GET /health`
- 数据源：`wss://ws-api.wolfx.jp/all_eew`
- 存储：PostgreSQL/PostGIS；首次启动会自动导入 `./data/subscriptions.json`
- 部署：单二进制或 Docker Compose

## 本地运行

本项目的生产构建固定使用 Go `1.25.12`。本地开发也建议使用同一版本：

```bash
cp config.example.yaml config.yaml
go mod download
go run . -config config.yaml
```

打开：

```text
http://127.0.0.1:30010
```

测试 Bark 链路：

```bash
go run . -config config.yaml -test-bark YOUR_BARK_KEY
```

## Docker 部署

镜像使用固定的 `golang:1.25.12-alpine3.23` 多阶段构建，不依赖本地预编译二进制。部署前先创建仅当前用户可读的配置和数据目录：

部署者必须先完成以下配置，示例域名不能直接用于生产：

- 默认 `bark.server` 为官方 `https://api.day.app`，官方 Bark 部署不需要设备数据库。
- 将 `server.public_url` 改成当前 EEW Relay 的 HTTPS 地址。
- 如果同时支持自建 Bark，再配置 `bark.self_hosted_server`，并提供只读 `bark.db` 或 `EEW_BARK_DEVICE_DSN`。
- 仅当自建 Bark 与 EEW Relay 位于同一容器网络时才设置 `bark.self_hosted_internal_server`。
- 默认 Wolfx WebSocket 是真实地震信息源；测试 Bark 推送时不要替换或模拟预警计算逻辑。

```bash
umask 077
cp config.example.yaml config.yaml
cp .env.example .env
sed -i "s/replace_with_a_long_random_hex_value/$(openssl rand -hex 32)/" .env
mkdir -p data
chmod 600 config.yaml .env
chmod 700 data

docker compose config --quiet
docker compose build --pull
docker compose up -d
docker compose ps
curl -fsS http://127.0.0.1:30010/health
```

查看日志时不要在工单或聊天中粘贴包含凭据的历史日志：

```bash
docker compose logs --tail=200 -f eew-bark
```

Compose 已启用 PostgreSQL/PostGIS、容器健康检查、只读应用根文件系统、capability 清理和 `json-file` 日志轮转。`./data` 保存 PostgreSQL、历史数据和审计数据。官方 Bark 模式不挂载 `bark.db`。

`docker-compose.yml` 默认只绑定：

```text
127.0.0.1:30010:30010
```

公网访问建议放在 Caddy、Nginx 或 Cloudflare Tunnel 后面。

## Caddy 反代

安装 Caddy 后：

```bash
sudo cp Caddyfile.example /etc/caddy/Caddyfile
sudo sed -i 's/eew.example.com/你的域名/g' /etc/caddy/Caddyfile
sudo systemctl reload caddy
```

Cloudflare DNS 中添加：

```text
类型: A
名称: eew
内容: 你的服务器 IP
代理状态: Proxied
```

SSL/TLS 模式建议使用 `Full (strict)`。如果源站使用 Caddy 自动签发证书，Cloudflare 到源站也可以走 HTTPS。

Cloudflare 官方说明：Cloudflare 可作为 DNS 和反向代理，开启代理后用户请求会先经过 Cloudflare 再到源站。文档：<https://developers.cloudflare.com/videos/onboard-domain-cf/>

## Cloudflare Tunnel 方案

如果不想暴露源站端口，可以使用 Tunnel：

```bash
cloudflared tunnel create eew-bark
cloudflared tunnel route dns eew-bark eew.example.com
cloudflared tunnel run eew-bark
```

Tunnel 的 ingress 指向：

```yaml
ingress:
  - hostname: eew.example.com
    service: http://127.0.0.1:30010
  - service: http_status:404
```

远程托管（remotely-managed）Tunnel 的 token 不要写入可被普通用户读取的 systemd unit，也不要长期放在进程命令行。把 token 保存到权限为 `0600` 的 root-only 文件，并让服务通过 `--token-file` 读取；token 一旦出现在日志、终端历史或 unit 文件中，应立即在 Cloudflare 后台轮换。

## 管理与模拟 API 鉴权

部署者可启用独立管理员后台。账号密码优先通过仅部署机可读的 `.env` 注入，不要写入公开仓库：

```dotenv
EEW_ADMIN_USERNAME=your_admin_username
EEW_ADMIN_PASSWORD=replace_with_a_long_unique_password
```

重启服务后访问 `https://eew.example.com/admin`。登录成功后，服务签发最长 24 小时、默认 8 小时有效的 HttpOnly、SameSite=Strict 签名会话 Cookie。管理员后台可以：

- 按 Bark Key/地点、服务器、通知等级、测活标签和订阅日期筛选，按表格列排序、分页查看和批量删除订阅；
- 一次校验并新增最多 100 个 Bark Key，且默认拒绝覆盖已存在订阅；
- 将同一真实地震的多个报次及不同台站消息归纳展示，查看震中、震级、深度、烈度、坐标、各报投递变化及脱敏逐用户明细；永久失效的 Bark Key 不计入累计投递失败，列表按 `失败数/失效 Key 数` 合并显示，逐用户明细支持全量排序后分页、状态/关键字筛选，并使用无需横向滚动的自适应列布局；
- 通过独立弹窗输入完整 Bark Key，或从订阅列表点“历史”直接打开弹窗，跨地震和报次查询该订阅在审计保留期内的成功、规则过滤、保护跳过、最终失败和失效记录，不占用或替换“通知历史”导航页；查询仅计算 Key 的 SHA-256 与审计哈希匹配，不发送通知、不回传明文 Key，订阅已删除后仍可查询已有历史；
- 对选中订阅或当前筛选结果只读测活，不触发 Bark/APNs 消息；测活后为每个订阅保存“设备可用”“设备 Token 缺失”“设备库缺失”“配置异常”或“官方未验证”标签，尚未检查或测活后又被修改的订阅显示“未测活”，可按标签筛选和排序。“设备可用”要求 Key 和非空设备 Token 同时存在；客户端删除自建服务器后保留空 Token 的记录会单独标记，但不会清理订阅或旧 Key。官方 Bark 没有无消息 Key 校验接口，因此只能标为“官方未验证”；标签保存在数据目录下权限为 `0600` 的 `subscription-liveness.json` 中；
- 只向指定的单个已订阅 Bark Key 发送链路或模拟地震测试；
- 查看 Wolfx 数据源、订阅存储、NATS 推送队列、审计目录和进程资源状态。
- 在独立“服务监控”页面按时间轴查看应用、Wolfx、PostgreSQL、NATS/JetStream、推送 Worker、官方/自建 Bark、Bark 设备数据库和审计存储状态。服务器每分钟采样一次，历史写入数据目录下的 `service-health.jsonl`，保留 30 天并支持最近 24 小时、7 天和 30 天视图；健康探测只使用连接检查、`/ping` 与 Worker 心跳，不发送通知。
- 在独立“Worker 管理”页面按节点查看任意数量的本机或远程 Worker，包括在线状态、运行/排空/暂停状态、并发、处理中批次、累计目标与失败数；管理员可通过 NATS 定向暂停、排空并暂停或恢复单个 Worker。控制面不会挂载 Docker Socket、不会停止容器或删除 JetStream 任务，并拒绝暂停最后一个运行中的 Worker；每次操作追加记录到审计目录的 `admin-worker-actions.jsonl`。
- 在“系统概览”中分别开关新生成通知的烈度信息和预估到达时间。开关同时作用于 Bark 标题/正文及通知详情网页，不改变烈度计算、订阅筛选或推送判定；详情页会保留通知生成时的设置，历史通知不回溯修改。设置持久化到数据目录下的 `notification-display.json`，默认两项均显示。

管理员批量新增不受公开订阅暂停开关影响，但仍执行 Bark Key、服务器、地点和通知规则校验。后台不会提供无确认的全用户群发按钮；原有 `POST /api/admin/simulate` 继续使用独立 `simulate_token`，只用于受控的全局模拟。

如果从旧版本迁移的订阅含有零宽、越界、重复或冲突的通知档位，可以先执行只读预检，再在完成数据库备份后运行一次性修复：

```bash
./eew-bark -config /app/config.yaml -repair-notification-bands-dry-run
./eew-bark -config /app/config.yaml -repair-notification-bands
```

修复器优先只调整错误档位，在相邻有效档位之间选择最接近默认配置的非重叠区间，并保留其余有效自定义档位；只有无法安全容纳时才将该订阅整组恢复为默认三档。PostgreSQL 写入使用单个事务和原更新时间条件，发现并发修改会整批回滚。正常运行路径不会为错误档位提供兼容回退。

Bark Key 同时是用户管理凭据。新页面和通知链接不再把 Key 放进服务器可见的 path 或 query：

```text
/manage#key=<URL 编码后的 Bark Key>
```

URL fragment 不会发送给服务器。Bark 通知打开 `/alert/{token}` 后，详情页会根据该短期 alert token 取得绑定的 Bark Key，自动填入管理界面并允许用户退订。详情 token 默认保留 24 小时，服务重启后失效。

用户管理 API 使用同一请求头：

```http
Authorization: Bearer <BarkKey>
```

例如：

```bash
read -rsp "Bark Key: " BARK_KEY; echo
curl -fsS https://eew.example.com/api/subscription \
  -H "Authorization: Bearer ${BARK_KEY}"
curl -fsS -X DELETE https://eew.example.com/api/unsubscribe \
  -H "Authorization: Bearer ${BARK_KEY}"
unset BARK_KEY
```

全局模拟只使用 `server.simulate_token`，并通过 header 发送，不再使用 query token：

```bash
read -rsp "Simulation token: " SIMULATE_TOKEN; echo
curl -fsS -X POST "https://eew.example.com/api/admin/simulate?kind=tiny" \
  -H "Authorization: Bearer ${SIMULATE_TOKEN}"
unset SIMULATE_TOKEN
```

带 Key 的旧路径仅用于短期兼容，不应继续生成、分享或写入监控规则。

## 大陆用户访问

Cloudflare 免费 CDN 在大陆网络下不保证低延迟，甚至可能出现运营商绕路。更稳的方案：

- 国内用户多：优先用国内服务器和已备案域名，接国内 CDN。
- 海外服务器加 Cloudflare：部署简单，但大陆访问速度不可控。
- 预警速度优先：后端服务器到 `ws-api.wolfx.jp` 和 Bark 推送服务器的延迟更关键；网页慢只影响订阅，不影响已订阅用户收到推送。

## 预警计算

```text
震中距 = haversine(订阅地, 震中)
震源距 = sqrt(震中距^2 + 震源深度^2)
```

P/S 波到达时间使用快速混合模型：

- 100 km 内：使用直达波固定速度估算，P 波默认 `6.0 km/s`，S 波默认 `3.5 km/s`。
- 100 km 以上：按震中距换算为角距，使用区域走时表插值估算 P 波和 S-P 时间。
- 深度修正：走时表以约 33 km 深度为参考，按当前震源深度和固定速度模型做轻量修正。
- 自动降级：缺少深度、距离超出走时表范围、插值结果异常时，自动降级为固定速度模型。

这样做的目的是避免远距离时固定速度模型把到达时间估得过晚。计算复杂度仍为 O(1)，每个订阅只进行一次球面距离、一次平方根和几十项以内的表插值，通常远小于 Bark HTTP 推送耗时。

本地烈度是经验估算，用于推送筛选，不等同官方烈度预报。P/S 到达时间也只是基于可获取数据的快速估算，不使用完整地壳速度结构、TauP、IASP91 或区域三维速度模型。

烈度估算保留四舍五入输出整数等级，但不再在 `M5.0`、`M6.0`、`M7.0` 处硬切换系数。服务会在相邻震级段之间线性平滑经验系数，避免实时首报震级轻微偏高时，中远场订阅地被分段跳变额外抬高 1 级。

## 跨台站合并与修订

服务按发震时间、震中距离、震级和来源事件编号，将 CENC、JMA、重庆、四川等不同台站对同一场地震的报文归纳为一个物理事件。消息队列保留首次接收时间并按顺序处理，因此最早到达的台站消息优先推送；后续报文中的来源、报次、发布时间、最终报标志、原始字段顺序或震中措辞变化不会单独触发推送。

修订始终与“上一次已完成且至少成功送达一条的版本”比较，小幅变化会累计。首轮扇出尚未结束、全部发送失败或全部订阅被筛选时不会产生修订；下一条符合通知条件的报文仍作为普通首条通知。默认仅在震级累计变化 `0.3`、震中移动 `10 km`、深度变化 `10 km`、发震时间变化 `3 秒`、最大烈度跨档或取消状态改变时再次推送，修订只在标题标记，正文直接显示最新信息。每次修订使用独立且确定性的 Bark 通知 ID，既不覆盖已经完成的首条通知，也不会因队列重投重复生成通知。不同机构的烈度表示会先换算到可比档位，避免仅因量表写法不同而重复推送。若同一时段存在两个同样可能匹配的近邻地震，系统会保守地保持为不同事件，避免错误合并造成漏报。

```yaml
alert:
  push_updates: true
  revision_magnitude_delta: 0.3
  revision_epicenter_km: 10
  revision_depth_km: 10
  revision_origin_seconds: 3
  dedup_keep_minutes: 120
```

`update_min_report_gap` 仅为旧配置兼容保留，真实地震更新不再由报次差决定。`push_updates: false` 会关闭普通修订推送，但安全相关的取消状态变化仍会推送；`ignore_cancel: true` 则继续完全忽略取消报。

## Bark 并发推送

收到真实 EEW 后，服务会先计算全部订阅者的距离、预计烈度和到达时间，再按优先级并发推送：

1. `critical` 优先，其次 `active`，最后 `passive`。
2. 同级别内优先推送 S 波 ETA 更短、预计烈度更高、距离更近的订阅地。
3. 按 Bark 服务器分组并发推送：官方 `api.day.app` 默认 `300` 并发；自建目标进入 NATS JetStream，由多个 worker 分批并发发送。worker 负责计算并发，Bark 侧通过全局 APNs 在途上限、有界 MySQL 池和 Token 缓存提供最终背压，避免增加 worker 时击穿设备库。

建议配置：

```yaml
alert:
  fanout_concurrency: 300
  self_hosted_fanout_concurrency: 750
  fanout_error_budget: 800
  key_failure_threshold: 3
  key_quarantine_minutes: 1440
  send_retry_attempts: 1
  send_retry_delay_ms: 300
  alert_detail_ttl_hours: 24
  alert_detail_max_items: 60000
```

Bark 官方服务器正常使用没有固定次数限制，但异常使用会触发 IP Ban：例如 5 分钟内超过 1000 次 400/404/500 等错误响应，或同一时刻建立超过 1000 条 TCP 连接。服务因此对官方服务器做了两层保护：

- 官方错误预算：5 分钟内官方 Bark 错误响应接近阈值时，临时停止后续官方 Bark 请求，避免触发 Ban。
- 官方单 Key 熔断：某个官方 Bark Key 连续返回 400/404 后，暂停该 Key 24 小时，避免失效 Key 在多次地震中反复消耗错误预算。

自建 Bark Server 不套用官方错误预算和官方 BAN 规则。网络错误、429、5xx，以及旧版 Bark 错误返回中明确的临时数据库饱和会被识别为临时错误。queue worker 会在 JetStream 消息保持未 ACK 的状态下逐目标抖动退避重试；404、BadDeviceToken 等永久错误不重试。实际速度仍取决于自建 Bark Server、APNs 和网络情况；高烈度订阅会优先进入各自服务器分组的并发队列。

扩展架构支持四部分：PostgreSQL 保存全部订阅和监测地点；NATS JetStream 保存短时推送任务并由多个 `-queue-worker` 竞争消费；Bark 设备注册可迁移到 MySQL；可选中继可按 Bark Key 稳定哈希承担受控比例的突发流量。每场地震都会评估全部订阅，不设置最大通知距离；是否通知仍由订阅地预估烈度和用户通知档位决定。旧配置中的 `alert.max_distance_km` 只为严格配置解析兼容而保留，运行时不再生效。任务 ACK 只在 worker 完成逐目标临时错误重试并返回最终批次结果后提交，通知 ID 由事件确定性生成，用于降低重投造成重复通知的概率。

队列 Worker 每隔 `queue.worker_heartbeat_seconds` 秒向独立的 NATS 核心主题发布一次只包含 Worker ID、启动实例 ID、节点 ID、运行状态、处理中数量、启动时间、并发数和累计处理量的心跳。管理员服务监控按 `queue.expected_workers` 判断缺失或过期心跳；心跳不进入 JetStream 任务流、不包含 Bark Key，也不会触发推送。`EEW_QUEUE_NODE_ID` 应在每台物理机或虚拟机上设置唯一稳定值，`EEW_QUEUE_WORKER_ID` 应在同一节点内保持唯一；未设置时会回退到主机名。项目提供的双 Worker Compose 建议配置：

```yaml
queue:
  expected_workers: 2
  worker_heartbeat_seconds: 10
```

仓库内的 `bark-server-patch/` 从固定的 Bark v2.3.5 commit 构建加固镜像。它增加有界 MySQL 连接池、设备 Token 读穿缓存、正确的 404/503 错误分类和进程级 APNs 在途上限。`ops/docker-compose.scale.yml` 已集成该镜像；连接池和缓存参数见 [bark-server-patch/README.md](bark-server-patch/README.md)。

部署者应根据实际 CPU、内存、网络和 Bark/APNs 投递能力设置 `subscription_limit`，并在提高容量前使用仓库内的 mock Bark 工具完成同规格压测。压测只覆盖应用到 mock Bark 的发送边界，不代表 APNs 最终到达时间保证。

如果 EEW 服务和自建 Bark Server 在同一个 Docker Compose 网络内，建议配置内部发送地址，避免预警 fanout 经过 Cloudflare/Tunnel 再回到同一台机器：

```yaml
bark:
  self_hosted_server: "https://bark.example.com"
  self_hosted_internal_server: "http://bark-server:8080"
```

本地可用 mock Bark 压测工具验证 fanout 调度能力，不会触达真实 Bark 或真实用户：

```bash
go run ./tools/fanoutbench -n 10000 -concurrency 750 -latency-ms 80 -jitter-ms 40 -transient-rate 0.01 -permanent-rate 0.005 -retry-attempts 1 -retry-delay-ms 300
```

压测应优先看 `duration`、`failed`、`retries` 和 P99。调整并发前，应在生产同规格机器上比较 `500`、`750`、`1000` 三档；如果出现连接重置、内存尖峰或 P99 明显上升，应回退到上一档。

## 订阅容量阈值

可以通过配置自动暂停新增订阅，现有订阅用户仍可更新 Bark Key 对应的地址和通知规则：

```yaml
server:
  subscription_limit: 0
  subscription_limit_message: "当前订阅人数已达到系统容量上限。为保障现有用户地震预警推送速度，已暂时停止新增订阅；现有已订阅用户不受影响，可正常接收地震预警。"
```

`subscription_limit` 为 `0` 时不启用自动阈值。设置为正数后，当当前订阅数大于等于该值时，`POST /api/subscribe` 会拒绝新的 Bark Key，并在 `/api/stats` 返回 `subscription_paused=true`、`subscription_paused_reason=limit` 和公告文案。手动 `subscription_paused: true` 的优先级更高。

## 投递审计

真实 EEW 事件会在 `server.audit_path` 写入持久化审计文件，默认路径为 `./data/audit`。模拟测试和历史地震测试不会写入审计。

每个事件会生成两类文件：

- `EVENT-rREPORT-TYPE.jsonl`：逐条订阅投递明细，包含 `pushed`、`filtered`、`skipped`、`failed` 和 `invalid_key` 状态，过滤/失败原因、Bark 服务器、通知级别、预估烈度、距离、ETA 和发送耗时。
- `EVENT-rREPORT-TYPE.summary.json`：事件汇总，包含总订阅数、入队数、过滤数、成功数、投递失败数、失效 Bark Key 数、官方/自建 Bark 分布、耗时 P50/P90/P99 和重试次数。历史审计在管理员接口读取时会按相同规则重新分类，不改写原文件。

审计明细不保存完整 Bark Key，只保存掩码和 SHA-256 哈希，便于后续按用户提供的 Key 计算哈希后定位记录。

## 官方与自建 Bark Server

服务按订阅保存 Bark 服务器地址，同时支持官方 `https://api.day.app` 和部署者配置的自建 Bark。

所有真实预警、修订、网页测试、管理员测试和命令行测试都会携带自定义 PNG 图标。发送层会为遗漏图标的调用自动补齐，并使用带版本号的静态 URL；更换版本号会让 Bark 设备端重新下载图标，避免旧缓存或首次下载失败长期回落为 Bark 默认图标。Bark 自定义图标需要 iOS 15 或更高版本。

- 只填写 Bark Key 时使用 `bark.server`，开源示例默认使用官方 Bark。
- 粘贴 `https://api.day.app/你的Key` 时明确使用官方 Bark。
- 粘贴已配置自建服务器生成的完整 URL 时使用该自建服务器。
- 新官方订阅无法读取官方设备库，因此先发送测试通知；只有官方接口确认发送成功后才保存正式订阅。
- 自建 Bark Key 仍通过部署者自己的 bbolt/MySQL 设备库验证。

官方 Bark 使用标准 POST 推送 URL，程序已有官方错误预算、失效 Key 熔断、有限重试和最高并发保护。Bark 官方项目的接口说明：<https://github.com/Finb/Bark>。

启用自建 Bark 的 bbolt 验证时，先在 `config.yaml` 配置 `bark.self_hosted_server`，再使用附加 Compose 文件挂载数据库：

```bash
export BARK_DB_PATH=/absolute/path/to/bark-data/bark.db
test -f "${BARK_DB_PATH}"
docker compose -f docker-compose.yml -f docker-compose.self-hosted.yml up -d
```

若使用 MySQL 设备库，只需设置 `EEW_BARK_DEVICE_DSN`，不需要挂载 `bark.db`。

下面是将 Bark Server 与本服务部署在同一环境的示例：

```yaml
services:
  bark-server:
    image: ghcr.io/finb/bark-server:latest
    ports:
      - "127.0.0.1:30011:8080"
    volumes:
      - ./data:/data
    command: bark-server --addr 0.0.0.0:8080 --data /data --max-apns-client-count 4
```

可以让 Cloudflare Tunnel 将 `bark.example.com` 转发到 `http://127.0.0.1:30011`。自建 Bark Server 只能替代 `api.day.app` 这一层，最终仍然要通过 Apple APNs 投递到 iPhone。

## Bark 点击跳转

默认点击通知会打开本服务的预警详情页：

```text
https://eew.example.com/alert/{token}
```

详情页适配手机端，会显示地震源信息、你的订阅位置、本地预估烈度、P/S 波预计到达时间，并提供这些入口：

- 苹果地图路线：从订阅地到震中，显示路线和距离
- 中国地震台网速报：在详情页内显示最近官方速报，并提供官方原网页入口

详情页 token 默认保留 24 小时。每次推送会为每个用户保存一条很小的内存详情记录；`alert_detail_max_items` 用于限制最大缓存条数，避免大规模预警后内存无限增长。默认 6 万条会保留最近的详情链接，超过后淘汰最旧链接；容量应按实际用户数、同日事件数和报告更新次数持续观察。

## 离线行政区解析

反向地理解析使用 PostgreSQL/PostGIS 中的中国行政区边界，不调用高德接口。边界数据来自 `AreaCity-JsSpider-StatsGov` 的 `2025.251231.260403` 版本，保留原始 GCJ-02 坐标；应用查询时只把输入的 WGS84 点转换为 GCJ-02。

默认 Docker Compose 已包含固定版本的 PostGIS 行政区数据镜像。首次使用空数据库目录启动时，镜像会自动写入与生产版相同的边界表和空间索引；部署者不需要下载、转换或手工导入行政区数据。应用更新不会重新导入或改变这份固定数据。

固定数据版本为 `2025.251231.260403`，Compose 同时锁定镜像 SHA-256 摘要，镜像标签被重新发布也不会静默替换数据。首次初始化需要解压边界数据并创建空间索引，因此 PostgreSQL 进入健康状态会比普通启动更慢；后续启动直接复用 `./data/postgres`，不会重复初始化。

参考数据包含省、市和区县级边界。`eew_admin_boundary_parts` 将高精度多边形预切为小块并建立 GiST 索引，避免大型几何在冷缓存下触发查询超时。地址正向搜索使用 Nominatim，并带全局限速和进程内缓存。

行政区名称按层级用空格显示，例如 `四川省 成都市 双流区`。每个解析结果带稳定的最低可用行政层级 ID；同一订阅不能添加两个属于同一最低层级的地点。缺少市、区县边界的区域按实际最低层级处理，例如当前台湾边界只有省级，因此台湾省内只能保留一个监测地点。搜索结果、地图点击、浏览器定位、订阅读取和订阅保存都会经过当前 PostGIS 边界重新解析。

通知规则在网页端按“烈度阈值及以上使用某提醒方式”配置，阈值下拉统一显示为 `2.0及以上` 这类一位小数选项。系统按阈值自动生成连续区间，例如 `2.0及以上` 勿扰静音不响铃、`3.0及以上` 强行响铃会生成 `[2.0, 3.0)` active 和 `[3.0, +∞)` critical。若较弱提醒的阈值不低于更强提醒，网页会从更强提醒阈值向下逐级调整，例如 critical 为 `3.0`、active 误选 `5.0` 时，active 自动调整为 `2.0`。最后一条规则自动作为开放式上限。

`ops/import-china-boundaries.sh` 只保留为维护者更换自定义数据集时的工具，正常开源部署不需要执行。若确实要自行替换数据，可在隔离数据库中先完成本地转换，再导入：

```bash
python tools/convert_areacity.py ok_geo.csv areacity.geojsonseq.gz
sudo AREACITY_GEOJSONSEQ_GZ=/path/to/areacity.geojsonseq.gz \
  /usr/local/sbin/eew-import-china-boundaries
```

导入脚本会校验文件大小、行政区层级、几何有效性和分块覆盖，再在单个事务中替换生产表。

## 备份与回滚

生产环境可以安装 `ops/eew-bark-backup.sh`、`.service` 和 `.timer` 做每日无停机快照。脚本直接导出 PostgreSQL 订阅库和 Bark MySQL 设备库，验证 PostgreSQL archive、MySQL devices 表及全部文件校验和，并保存部署配置、服务健康历史、通知展示设置和订阅测活标签。迁移期的 JSON 和 bbolt 文件只作为旧版回滚材料附带保存，不再是主数据源。备份目录和文件分别为 `0700`、`0600`，默认保留 14 天。

该定时任务只提供同机恢复点，不是异地备份。PostgreSQL 使用一致性逻辑备份，MySQL 使用 `--single-transaction` 导出；生产环境仍应把通过校验的备份同步到加密的异地存储。

每次部署前至少备份 `config.yaml` 和 `data/`，并保留当前容器镜像。下面的短暂停服能得到一致的应用数据快照：

```bash
cd /opt/eew-relay
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
BACKUP="/var/backups/eew-relay/${STAMP}"
umask 077
mkdir -p "${BACKUP}"

docker tag "$(docker inspect --format '{{.Image}}' eew-bark)" "eew-bark:rollback-${STAMP}"
docker compose stop eew-bark
cp -a config.yaml docker-compose.yml data "${BACKUP}/"
docker compose start eew-bark
printf '%s\n' "${STAMP}" > "${BACKUP}/rollback-tag"
```

普通代码回滚只需要恢复旧镜像，不要回退 `data/`，否则会丢失部署后新增或更新的订阅：

```bash
docker tag "eew-bark:rollback-${STAMP}" eew-bark:local
docker compose up -d --no-build --force-recreate eew-bark
curl -fsS http://127.0.0.1:30010/health
```

只有确认订阅或审计数据损坏时才恢复数据快照。恢复前先再次备份当前数据库并停止写入，使用 `pg_restore` 恢复 `eew-postgres.dump`、使用 MySQL 客户端恢复 `bark-mysql.sql`，再启动服务并检查 `/health`、`/api/stats`、NATS worker 连接和 Bark Key 验证。不要用迁移前的 JSON/bbolt 覆盖当前 PostgreSQL/MySQL，否则会丢失切换后的新增数据。

如果要强制覆盖 Bark 点击链接，可以设置：

```yaml
alert:
  click_url: "weixin://"
```

详情页“中国地震台网”默认使用中国地震台网速报目录：

```text
https://data.earthquake.cn/datashare/report.shtml?PAGEID=earthquake_subao
```

由于官方页面偏桌面布局，详情页会额外通过 `/api/official-reports` 拉取最近速报并用移动端卡片展示核心字段。

## 注意

Wolfx 是第三方聚合数据源，Bark/APNs 不是硬实时链路。这个服务适合个人或小范围预警辅助，不应作为唯一生命安全系统。

开源版本不包含任何维护者生产域名或生产凭据。部署和测试应使用独立 Bark Server、独立数据库及独立推送 Key。第三方组件和地图数据说明见 [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md)，安全问题报告方式见 [SECURITY.md](SECURITY.md)。

## 许可证

本项目使用 [MIT License](LICENSE)。
