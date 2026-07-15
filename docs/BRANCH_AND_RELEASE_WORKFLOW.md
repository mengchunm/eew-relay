# 分支与发布工作流

本项目采用“共享代码只修改一次，生产差异使用覆盖层维护”的方式。

## 分支职责

| 分支 | 用途 | 是否同步 GitHub | 允许内容 |
| --- | --- | --- | --- |
| `main` | 可独立部署的开源版本 | 是，只同步此分支 | 通用地震处理、地图、定位、规则、官方与自建 Bark 支持、开源部署文件和公共文档 |
| `production` | 维护者实际生产环境 | 否，仅保存在本地 Git 仓库和受控备份中 | `main` 的全部通用能力、生产私有功能、关停/访问策略和生产部署覆盖层 |
| `backup/*` | 合并或部署前的临时恢复点 | 否 | 只用于本地回退，不作为长期开发分支 |

不要把 `production` 推送到公共远端，也不要把生产域名、凭据、数据库路径、订阅数据或私有功能复制到 `main`。

## 日常修改顺序

通用功能、Bug 修复或真实地震处理逻辑应先在 `main` 完成：

```powershell
git switch main
git pull --ff-only origin main
go test ./...
# 修改、测试、提交
git push origin main
```

通过评审后，再把同一提交合并到生产分支：

```powershell
git switch production
git status --short
git merge --no-ff main
go test ./...
```

如果合并触及生产界面、订阅创建规则、Bark Key 验证、推送目标、数据库或部署文件，必须逐项对照生产契约并由维护者验收，不能直接部署。

生产专属改动只在 `production` 提交。不要把整个生产提交反向合并到 `main`；确实需要开源的通用修复，应重新整理成不含生产信息的独立提交，再放入 `main`。

## 每次变更必须同步的内容

以下任一内容发生变化时，代码、测试和文档必须在同一提交或同一组连续提交中更新：

- 用户可见页面、按钮、提示语或访问条件；
- Bark Key 验证、测试、订阅、更新或取消流程；
- 地图、定位、地址解析或地震数据源；
- 配置项、环境变量、端口、依赖服务或持久化目录；
- Docker Compose、镜像、构建、部署、备份或回滚步骤；
- `main` 与 `production` 的职责边界。

提交前执行：

```powershell
go test ./...
git diff --check
git status --short
```

开源部署变化应更新 [OPEN_SOURCE_DEPLOYMENT.md](OPEN_SOURCE_DEPLOYMENT.md) 和根目录 `README.md`。生产覆盖层变化应更新 `ops/production/README.md` 及生产行为契约。

## 开源发布检查

推送 `main` 前确认：

1. 不包含生产域名、密钥、用户数据或私有管理能力。
2. `config.example.yaml`、`.env.example` 和 Compose 文件可以组成完整示例。
3. 使用真实 Wolfx 地震数据源，测试部署使用隔离的 Bark、数据库和域名。
4. `go test ./...`、`go vet ./...` 和 GitHub Actions 均通过。
5. README、部署文档和变更检查清单已同步。

## 生产发布检查

生产发布必须从干净的 `production` 分支生成本地构建产物，并遵守生产覆盖层文档。部署前保留配置、数据和旧镜像回滚点；部署后检查应用与 Worker 镜像一致、Wolfx 已连接、队列正常、订阅数合理，并等待维护者验收后再清理临时产物。

