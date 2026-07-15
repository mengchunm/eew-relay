# Contributing

感谢参与 EEW Relay。提交前请先阅读：

- [分支与发布工作流](docs/BRANCH_AND_RELEASE_WORKFLOW.md)
- [开源版本部署指南](docs/OPEN_SOURCE_DEPLOYMENT.md)
- [变更检查清单](docs/CHANGE_CHECKLIST.md)

通用修复和功能进入公开 `main` 分支。维护者生产环境的域名、凭据、数据、访问策略和私有功能不得提交到公共仓库。

任何用户可见行为、API、配置或部署流程变化都必须同步维护对应文档和测试。仅修改代码而不更新已受影响文档的提交视为未完成。

最低验证要求：

```bash
go mod verify
go mod tidy -diff
go test ./...
go vet ./...
git diff --check
```

安全问题不要公开提交包含 Bark Key、订阅数据、服务器凭据或漏洞利用细节的 Issue；请按 [SECURITY.md](SECURITY.md) 中的方式报告。

