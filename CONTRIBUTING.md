# Contributing

感谢你帮助改进 AI Gateway。欢迎提交文档、测试、协议兼容性和错误处理方面的改进。

## 报告问题

请提供：

- 网关版本、操作系统和上游协议类型。
- 最小请求及期望 / 实际结果；能使用本地 mock 复现最好。
- 流式问题请附脱敏的 SSE 事件顺序，注明是否出现结束事件。
- **不要提交密钥、真实对话正文、私有地址或未经检查的数据库。**

安全漏洞请遵循 [SECURITY.md](SECURITY.md)，不要在公开 Issue 发布利用细节。

## 本地开发

```bash
cargo test
cargo build --locked
cargo clippy --all-targets -- -D warnings
```

在不同终端启动本地模拟服务和测试网关：

```bash
python3 dev/mock_upstream.py 19100
cargo run -- --config dev/test-config.toml
```

然后运行：

```bash
python3 dev/smoke_test.py
python3 -m pip install openai anthropic
python3 dev/sdk_test.py
```

集成测试会对测试配置执行保存 / 重载，并在配置所在目录创建数据库和备份。使用测试配置，不要将它们指向你的私人或生产配置。CI 使用独立的临时目录。

## Pull request

1. Fork 仓库，为一个明确问题建立分支。
2. 修复缺陷前先添加能复现问题的测试。
3. 保持现有代码风格，不把无关重构混进修复。
4. 协议转换变更同时考虑流式、非流式和组合转换路径。
5. 更新相应的文档和 `CHANGELOG.md`，在 PR 中写明验证方式。

提交贡献表示你同意其按本项目的 [MIT License](LICENSE) 发布。

---

For English-speaking contributors: include a minimal redacted reproduction, add a regression test before a bug fix, run the commands above, and keep unrelated changes out of the PR. Local integration tests use mock providers; do not run them against production configuration. Contributions are licensed under MIT.
