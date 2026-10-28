# Win11 功能测试手册

本文用于在 Win11 开发机上验证当前已部署的 `realtime-dw` 主链路。推荐通过 SSH 使用 R630 的 K3s 环境测试，不要为了本地调试把 Kafka、HBase、Doris 暴露到公网，也不要在 ThinkBook 上一次启动全部中间件和 41 个示例入口。

本手册的首个目标是验证一条可观察、可清理的链路：

```text
Win11 发送测试事件 -> Kafka mall-events-log -> Flink EtlJob01
  -> Kafka dwd-events-log + Doris dwd.mall_events_wide
```

这会验证当前真正运行的业务作业和中间件，不等同于所有教学 Demo 都已经具备运行条件。

## 1. 适用范围与当前限制

- 已验证的线上归属是：Kafka/HBase/Doris 在 `middleware`，Flink 作业 `realtime-dw-etl-01` 在 `realtime-dw`。详见 [K3s 迁移测试报告](k3s-migration-test-report-2026-10-25.md)。
- 本次仅以 `com.test.bigdata.rtdw.data_etl.EtlJob01_UserEventsLogCommonDim` 作为端到端验收作业。它已经支持 `KAFKA_BOOTSTRAP_SERVERS`、`HBASE_ZOOKEEPER_QUORUM`、`DORIS_FENODES`、Doris 账号和 checkpoint 路径的环境变量覆盖。
- `EtlJob02`–`EtlJob07`、大部分 `rt_rule_mk` Demo、Spark 批作业和若干测试类仍硬编码 `doitedu`、MySQL 或 HTTP 服务；在没有先补齐连接参数、建表和初始化数据前，**不要**把它们的连接异常判成代码功能失败。
- Maven 当前只有 `WindowTest`，Surefire 会发现 0 个断言测试。因此 `mvn test` 是构建门槛，不是端到端功能证明。
- 当前 Maven Jar 不是可独立运行的 fat Jar；不要使用 README 中旧的 `java -cp xxx.jar 主类` 方式作为生产功能测试。集群作业由镜像中的 Flink 依赖和 `business-etl-job` ConfigMap 中的业务 Jar 共同组成。

## 2. Win11 前置条件

在 PowerShell 进入仓库根目录，确认 JDK、Maven 和 SSH：

```powershell
Set-Location D:\develops\git\github\java\doit36_realtime_project
java -version
mvn -v
ssh -V
Test-NetConnection k3s-master -Port 22
```

要求：

- `java -version` 为 JDK 17；IDEA 的 Maven Runner 也应选用 JDK 17。
- `k3s-master` 是可由 Win11 SSH 配置解析的主机别名；若未配置，请改为实际管理 IP 或在 `C:\Users\<用户名>\.ssh\config` 中配置。
- 账号须能执行远端 `k3s kubectl`。所有下文命令只读取集群状态或向测试 Topic 写入一条标有唯一 ID 的记录。

## 3. 第一层：本机编译门槛

先完成不依赖 Kafka/HBase/Doris 的检查：

```powershell
mvn -B -ntp test
```

预期：5 个 Maven 模块均为 `BUILD SUCCESS`。如果失败，先修复编译、依赖或 JDK 问题；不要继续排查远端中间件。

只验证实时模块时使用：

```powershell
mvn -B -ntp -pl realtime_dw -am -DskipTests package
```

## 4. 第二层：确认现场基线

以下命令不修改集群。应看到各 Deployment 的 `READY` 为 `1`、Flink 为 `RUNNING STABLE`，且旧 `doit` Namespace 不存在：

```powershell
ssh k3s-master 'k3s kubectl -n middleware get deploy business-hbase business-doris-fe business-doris-be kafka'
ssh k3s-master 'k3s kubectl -n realtime-dw get flinkdeployment realtime-dw-etl-01'
ssh k3s-master 'if k3s kubectl get namespace doit --no-headers 2>/dev/null; then exit 1; else echo doit_namespace=absent; fi'
```

如 Flink 不是 `RUNNING/STABLE`，先看 JobManager/TaskManager 状态和日志，勿向输入 Topic 投送验收数据：

```powershell
ssh k3s-master 'k3s kubectl -n realtime-dw get pods -o wide'
ssh k3s-master 'k3s kubectl -n realtime-dw get events --sort-by=.lastTimestamp | tail -n 30'
```

可在另一个 PowerShell 窗口打开 Flink UI 隧道。保持该窗口运行，再访问 `http://127.0.0.1:8081`：

```powershell
ssh -L 8081:127.0.0.1:8081 k3s-master 'k3s kubectl -n realtime-dw port-forward service/realtime-dw-etl-01-rest 8081:8081'
```

在 UI 中确认作业处于 Running，并在提交事件后确认有持续成功的 checkpoint；不要通过 UI Stop 这个正在运行的共享作业。

## 5. 第三层：Kafka -> Flink -> Kafka/Doris 功能验收

### 5.1 写入一条带唯一标识的测试事件

在 PowerShell 创建短 ID。`eventId` 需不超过 Doris 现有表的 `VARCHAR(10)`：

```powershell
$EventId = 'w' + (Get-Date -Format 'MMddHHmm')
$SessionId = "win11-$EventId"
$Payload = @"
{"username":"win11user","session_id":"$SessionId","eventId":"$EventId","eventTime":1792868000000,"lat":31.2304,"lng":121.4737,"release_channel":"win11-test","device_type":"desktop","properties":{"url":"https://example.invalid/win11-test"}}
"@
$Payload | ssh k3s-master 'k3s kubectl -n middleware exec -i deploy/kafka -- /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka.middleware.svc.cluster.local:9092 --topic mall-events-log'
```

记录 `$EventId`，后续所有查询都以它筛选。该消息会写入共享测试链路，故不要使用真实用户信息、生产账号或敏感 payload。

### 5.2 验证 Flink 输出 Topic

等待约 10–20 秒后，消费 `dwd-events-log` 并筛选刚才的 ID：

```powershell
$DwdEvents = ssh k3s-master 'k3s kubectl -n middleware exec deploy/kafka -- /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka.middleware.svc.cluster.local:9092 --topic dwd-events-log --from-beginning --timeout-ms 15000'
$DwdEvents | Select-String -SimpleMatch $EventId
```

预期：输出至少有一行 JSON 包含 `$EventId`。Kafka consumer 在等待超时后可能返回非零退出码；只要筛选结果存在，这不视为测试失败。

若没有结果，按此顺序排查：Flink UI/Job 状态、JobManager 日志、`mall-events-log` 输入是否写入、HBase/Doris 服务是否 Ready。不要重复大量投递相同事件。

### 5.3 验证 Doris 明细结果

在另一个 PowerShell 窗口建立仅本机可见的 Doris 隧道：

```powershell
ssh -L 9030:127.0.0.1:9030 k3s-master 'k3s kubectl -n middleware port-forward service/business-doris-fe 9030:9030'
```

安装 MySQL 8 客户端后，在第三个窗口运行。`-p` 会交互式提示输入密码；请从受管的 `middleware/business-doris-auth` Secret 获取权限，不要把密码写进 PowerShell 历史、文档或 Git：

```powershell
mysql.exe -h 127.0.0.1 -P 9030 -u doit_etl -p -e "SELECT username, session_id, event_id, event_time FROM dwd.mall_events_wide WHERE event_id = '$EventId' ORDER BY event_time DESC LIMIT 5;"
```

预期：查到 `username=win11user` 且 `event_id=$EventId` 的行。`session_id` 是 `VARCHAR(20)`，测试值必须保持较短，避免把字段截断误判为链路错误。

## 6. 通过与失败判定

| 检查点 | 通过标准 | 失败时优先检查 |
| --- | --- | --- |
| Maven | 5 个模块 `BUILD SUCCESS` | IDEA/Maven 使用的 JDK 是否为 17 |
| K3s 基线 | Kafka/HBase/Doris `1/1`，Flink `RUNNING/STABLE` | Pod 事件、PVC、镜像和 JobManager 日志 |
| 输入 Kafka | producer 正常结束且无连接异常 | SSH 权限、Kafka Pod、Service DNS |
| 输出 Kafka | 找到唯一 `$EventId` | Flink 作业状态、checkpoint、HBase lookup |
| Doris | 按 `$EventId` 查到一行 | Doris FE/BE、账号权限、Flink sink 日志 |

满足最后两项才可称为当前实时 ETL 的功能测试通过。Maven 通过或 Flink Pod Running 都不能单独替代端到端结果。

## 7. 清理、留档与下一步

- 本测试事件可保留为短期可核验证据；需要清理时，用受控 Doris 客户端执行 `DELETE FROM dwd.mall_events_wide WHERE event_id = '<记录的 EventId>'`，并记录操作人和时间。Kafka 采用保留策略，通常不做逐条删除。
- 将实际的 `$EventId`、执行时间、Maven 输出、Flink 截图和 Doris 查询结果保存到测试记录中。
- 不要为了“全量测试”并行启动所有 `main`。先为每个仍写死 `doitedu` 的作业完成连接参数化、Topic/表的隔离命名和测试数据初始化，再按“一个输入 Topic + 一个作业 + 一个预期输出”的方式逐个验收。
- 对源码修改的回归验证，应新建隔离的 FlinkDeployment 和测试 Topic；不要在未保存 checkpoint/savepoint 的情况下替换共享 `realtime-dw-etl-01` 的 Jar。
