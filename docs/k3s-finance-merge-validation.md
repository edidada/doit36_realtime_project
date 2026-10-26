# master -> k3s 合并与历史 CI 修复

先在 `master` 完成 FIN-OLAP / FIN-RISK 编码并提交 `4bcd555`，再将该提交合并到 `k3s`。`master` 对应的 Maven Actions `40355155447` 已成功，包含 Linux 全 Reactor 与 Windows 金融模块回归。

使用 `gh run view 40187831471 --log-failed` 查到旧 K3s 镜像构建在 Linux/macOS 均失败，具体是 `EtlJob01_UserEventsLogCommonDim` 的历史合并遗留：

- 不存在的 `sqlValue`、`environmentOrDefault` 方法调用。
- 重复声明 `dorisPassword`，重复导入 `StatementSet`。
- Kafka 和 HBase DDL 中连接选项重复，部分 sink 仍使用写死地址。
- 恢复成两个独立 INSERT execute，破坏了 Application Mode 的单次执行要求。

本次在 `k3s` 合并时统一使用 `RuntimeConfig.sql/value`，保留系统属性/环境变量/私有本地配置的读取能力和 SQL 单引号转义；合并 checkpoint 配置，恢复同一个 StatementSet 中的 Kafka/Doris 双输出。增加了不启动连接器的 JUnit 5 回归测试，验证覆盖后的输入/输出 Topic、Kafka、HBase、Doris 地址与含引号的测试密码都能正确进入表配置。测试没有访问现网服务。

金融模块自身实现和全部 38 个测试与 `master` 相同。`k3s` 额外增加一个 ETL 配置回归测试，7 个 Maven 工程应一起执行 `mvn -B -ntp verify`。K3s 镜像工作流仍只打包原有 Flink 模块，金融库没有自动部署到共享集群。

另外读取了历史 Semgrep 运行 `40171573343` 的 SARIF：其中 17 个阻断项涉及可变 action 标签、现有 hostNetwork/root 容器、Java 反序列化和动态 Groovy。Maven 工作流已固定 action SHA；其余历史安全架构项不属于此次金融模块功能验收，仍需要专项改造。没有通过关闭规则或忽略扫描来掩盖这些项。

新提交的 CI 结果请以关联提交的 GitHub Actions 运行记录为准，历史 CI 不能替代新合并提交的验收。
