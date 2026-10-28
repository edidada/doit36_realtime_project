# 2026-10-29 本地实际运行日志

六份 `.log` 均由 Log4j2 XML 的 RollingFile appender 实际生成，非手写结果；已检查内容不含 Bearer token、密码或私有业务数据。保留机器上的 JDK/Maven/JAR 路径作为运行环境证据，不对外承诺这些路径可直接复用。

- `realtime_dw.log`、`rt_rule_mk.log`、`batch_jobs.log`、`redis_demo.log`：只验证日志 provider、Core 版本、XML 配置和落盘，不表示已启动外部中间件或完成对应业务作业。
- `fin-olap-engine.log`：日志探针 + OLAP 合成 CSV 实际运行统计。
- `fin-risk-engine.log`：日志探针 + 用户指定 Microsoft JDK 17 实际运行可执行 JAR 的 8 个 QuantLib 公开样例 + Maven 显式参数运行首例的结果。

完整命令、公开数据出处及验收边界见 [运行说明](../../../docs/log4j2-and-risk-run-guide.md)。重复执行会追加新日志；提交前再次检查，不能将真实支付/账户日志整目录加入 Git。
