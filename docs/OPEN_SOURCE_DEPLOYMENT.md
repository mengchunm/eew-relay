# 开源版本部署指南

本指南适用于 GitHub `main` 分支。开源版本是可独立运行的正式订阅服务，不包含维护者生产环境的关停策略、私有管理功能、域名、凭据或数据。

## 部署条件

- Linux amd64/arm64 服务器，建议至少 2 核 CPU、4 GB 内存和 20 GB 可用磁盘；
- Docker Engine 和 Docker Compose v2，或 Go `1.25.12`；
- 可访问 `wss://ws-api.wolfx.jp/all_eew`、Bark/APNs、地图和地址搜索上游；
- 一个 HTTPS 域名和反向代理；
- PostgreSQL/PostGIS 持久化目录；
- 使用自建 Bark 时，需要只读 Bark bbolt 数据库或 Bark MySQL 设备库连接；
- 独立测试环境，不能复用其他生产项目的服务器、域名、数据库或 Bark Key。

浏览器定位要求安全上下文。公网部署应使用 HTTPS；HTTP 页面无法可靠调用设备定位时，用户仍可通过地址搜索或点击地图添加位置。

## 获取与检查代码

```bash
git clone https://github.com/mengchunm/eew-relay.git
cd eew-relay
git switch main
git status --short
go test ./...
```

确认当前提交和发布说明后再部署，不建议直接长期跟随未固定的远端分支头。

## 配置

```bash
umask 077
cp config.example.yaml config.yaml
cp .env.example .env
password="$(openssl rand -hex 32)"
sed -i "s/replace_with_a_long_random_hex_value/${password}/" .env
unset password
if grep -q 'replace_with_' .env; then
  echo "请先替换 .env 中的示例凭据" >&2
  exit 1
fi
mkdir -p data
chmod 600 config.yaml .env
chmod 700 data
```

至少检查这些配置：

- `bark.server` 与 `bark.self_hosted_server`；
- `server.public_url`；
- `server.subscription_paused` 与 `server.subscription_limit`；
- `wolfx.websocket_url`，正常部署保持真实 Wolfx 数据源；
- `alert.revision_*` 跨台站事件修订阈值；未配置时使用示例文件中的保守默认值；
- `queue.expected_workers` 应与实际启动的推送 Worker 数量一致，供管理员服务监控识别 Worker 缺失；
- PostgreSQL 密码和 Bark 设备库只读连接；
- 反向代理的 HTTPS、请求头和真实客户端 IP 配置。

示例值不能直接用于公网。必须替换 `.env` 的 PostgreSQL 占位密码，并把 `config.yaml` 中的示例域名改为实际 HTTPS 地址。凭据只放在本机受限文件或密钥管理系统中，不提交 Git。

## Docker Compose 部署

默认 Compose 使用 GitHub Container Registry 的预构建应用镜像：

```bash
docker compose config --quiet
docker compose pull eew-bark postgres
docker compose up -d
docker compose ps
curl -fsS http://127.0.0.1:30010/health
```

默认应用镜像为 `ghcr.io/mengchunm/eew-relay:latest`。维护者通过 GitHub Actions 在 `main` 更新时发布 `latest`、`main` 和不可变 `sha-...` 标签；如果 GHCR package 尚未设置为 Public，部署机需要先登录 GHCR。首次拉取 PostgreSQL 行政区镜像仍会下载固定的地理数据层；首次初始化边界和空间索引也需要更长时间，后续启动会复用持久化目录。

如需从源码构建应用，使用源码覆盖文件，不要修改默认 Compose：

```bash
docker compose -f docker-compose.yml -f docker-compose.source.yml build --pull eew-bark
docker compose -f docker-compose.yml -f docker-compose.source.yml up -d
docker compose -f docker-compose.yml -f docker-compose.source.yml ps
```

源码构建会下载 Go 工具链和模块，网络较慢时仍可能耗时；普通部署不需要执行这组命令。

如果使用自建 Bark bbolt 数据库：

```bash
export BARK_DB_PATH=/absolute/path/to/bark-data/bark.db
test -f "$BARK_DB_PATH"
docker compose -f docker-compose.yml -f docker-compose.self-hosted.yml up -d
```

如果使用 Bark MySQL 设备库，设置 `EEW_BARK_DEVICE_DSN`，不要同时给应用写权限。

大规模自建 Bark fanout 建议使用仓库内 `bark-server-patch/` 构建的加固镜像；`ops/docker-compose.scale.yml` 已包含完整示例。该镜像固定 Bark v2.3.5 上游 commit，并为 MySQL 查询提供连接池背压和 Token 缓存。不要仅通过无限提高 MySQL `max_connections` 承接突发请求。

## HTTPS 与反向代理

应用默认只监听宿主机 `127.0.0.1:30010`。使用 Caddy、Nginx 或 Cloudflare Tunnel 提供 HTTPS，不要直接暴露数据库、NATS、Bark 数据库或管理端口。

上线后检查：

- 首页、地图瓦片、地址搜索、反向地理解析和浏览器定位；
- 官方 Bark 与自建 Bark 的测试通知；
- 新订阅、更新、查询和取消订阅；
- `/health` 中 Wolfx 连接和队列状态；
- `/api/stats` 中订阅暂停状态和容量限制；
- 容器重启后订阅、行政区和审计数据仍存在。

## 更新

```bash
git fetch origin
git switch main
git pull --ff-only origin main
docker compose pull eew-bark
docker compose up -d --no-deps --force-recreate eew-bark
curl -fsS http://127.0.0.1:30010/health
```

涉及数据库、Compose、环境变量或行政区镜像的更新，应先阅读对应提交和 README，不能只重建应用容器。

## 备份与回滚

更新前至少备份：

- `config.yaml` 和 `.env`；
- PostgreSQL 数据或逻辑备份；
- Bark 设备库配置；
- 当前应用镜像 ID；
- 审计和历史数据目录。

普通代码回滚优先恢复旧镜像，不要直接回退数据库目录。只有确认数据损坏时才恢复数据备份，并在恢复前再次保存当前状态。

## 验收清单

- [ ] 页面通过 HTTPS 打开，无生产项目域名或私有入口；
- [ ] 地图、搜索、定位和经纬度转区域可用；
- [ ] Wolfx 真实地震 WebSocket 已连接；
- [ ] Bark 测试、订阅、更新和取消流程符合开源版本说明；
- [ ] PostgreSQL/PostGIS、Bark 设备库和审计目录持久化正常；
- [ ] 重启应用后健康检查恢复；
- [ ] 备份和镜像回滚步骤已实际演练。
