# Java main 日志

从项目根目录运行：

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
bash scripts/java-mains.sh --prepare
bash scripts/java-mains.sh cn.doitedu.rtmk.demo8.RuleEngineRecovery
```

`--prepare` 只编译，不会启动中间件或创建 hosts 映射。启动脚本现在默认读取本机
`.artifacts/main-audit/k3s/runtime-env.json`，也可通过 `MAIN_ENV_JSON` 指定其他 JSON。
配置缺失时，需自行设置 Kafka、MySQL、Redis 等环境变量，否则会回退到课程默认地址。
该私有文件含凭据，不加入 Git。MySQL/Redis 的 SSH 隧道仍需保持连接。

IDEA 的 Working directory 设为项目根目录，VM options 使用
`@$PROJECT_DIR$/scripts/java17.options`（也可填绝对路径）。两个业务模块的
RuntimeConfig 会读取同一私有 JSON，JVM 属性和环境变量优先。

三个 Java 模块使用 `src/main/resources/log4j2.xml`，同时输出控制台和滚动文件。
脚本启动时，实时文件为 `logs/runtime/<完整类名>.log`；IDEA 默认文件为
`logs/runtime/application.log`，可用 `-Dmain.class=<完整类名>` 区分。
实时目录加入忽略规则，避免原始运行信息意外进入 Git；`validation/` 中保存的是
本次真实执行的脱敏快照，已加入 Git 跟踪。快照还捕获 System.out/System.err 和异常堆栈。

全量复跑（会真实访问、写入测试环境，先完成 prepare）：

```bash
bash scripts/java-mains.sh --all --seconds 90
```

包含 main 和 test 目录的全部 42 个 Java main，不含 Scala main。
流任务达到两个成功 checkpoint 或到达最长观察时间后主动发送 SIGTERM；
`actively_stopped=true`、退出码 143 不代表正常退出，也不等同完整业务验收。
结果见 `validation/results.json`，每项记录退出码、是否主动停止、是否看到 RUNNING、
成功 checkpoint 数和 ERROR 行数。超时但未 RUNNING 的任务不能判定启动成功。
有限任务以退出码 0 判断；错误日志保留，不会为了全绿而删除。
原先缺失的 HTTP 语义服务和 Socket 服务仍需实际部署，详见
[执行验收说明](../docs/k3s-main-execution.md)。

本次还单独使用上面的无环境变量命令运行 RuleEngineRecovery，实际解析为
Kafka `192.168.31.211:9092`、MySQL `127.0.0.1:13306`，完成两个 checkpoint，
没有 UnknownHostException。证据：`default-RuleEngineRecovery.log`；随后主动停止，退出码 143。

全量快照这一轮使用 `JAVA_TOOL_OPTIONS=-XX:TieredStopAtLevel=1` 减少本机 JIT 启动开销；
该选项仅用于本轮检查，没有写入正常启动脚本。
