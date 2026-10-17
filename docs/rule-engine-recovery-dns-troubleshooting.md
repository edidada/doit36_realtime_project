# RuleEngineRecovery：`doitedu` DNS 失败与全局 Failover 排查

## 现象与根因

日志中的关键信息为：

```text
bootstrap.servers = [doitedu:9092]
WARN Couldn't resolve server doitedu:9092 ... DNS resolution failed for doitedu
Caused by: org.apache.kafka.common.config.ConfigException:
No resolvable bootstrap urls given in bootstrap.servers
```

根因不是 Flink 或 Kafka 客户端本身，而是作业实际运行环境无法解析硬编码主机名
`doitedu`。

`cn.doitedu.rtmk.demo8.RuleEngineRecovery` 至少有两处依赖该主机名：

| 数据源 | 当前配置 | 代码位置 |
| --- | --- | --- |
| Kafka source（`log-source`） | `doitedu:9092` | `RuleEngineRecovery.java` 的 `.setBootstrapServers("doitedu:9092")` |
| MySQL CDC source | `doitedu:3306` | CDC DDL 的 `'hostname' = 'doitedu'` |

Kafka source 初始化失败后，Flink 的 failover 按整个作业恢复，而不是只重启出错的
source，所以两个 source 和后续算子都会跟随作业反复重启。即使先修好 Kafka，MySQL
CDC 仍可能因同一个 DNS 问题失败。

## 先在 Pod 内验证

不要只在 K3s master 或开发机上测试；DNS 与网络必须从实际 JobManager/TaskManager Pod
内确认。将 `<taskmanager-pod>` 替换为实际 Pod 名称：

```bash
kubectl -n doit exec <taskmanager-pod> -- getent hosts doitedu
kubectl -n doit exec <taskmanager-pod> -- sh -c 'nc -vz doitedu 9092; nc -vz doitedu 3306'
```

判断方式：

- `getent hosts` 没有输出或返回非零：是 DNS/hosts 配置问题；
- DNS 成功、端口失败：检查安全组、防火墙、路由和服务是否监听；
- Kafka bootstrap 端口成功、消费仍失败：继续检查 Kafka `advertised.listeners`；
- Kafka 正常后作业仍重启：查看 MySQL CDC 的 `doitedu:3306`、账号权限及 binlog 配置。

## 推荐处理方案

### 1. 将连接信息改为部署配置

长期方案是移除 Java 源码中的硬编码地址，改用 Flink 参数、环境变量或配置文件，例如：

```text
KAFKA_BOOTSTRAP_SERVERS=<kafka-host>:9092
MYSQL_HOST=<mysql-host>
MYSQL_PORT=3306
```

不同开发、测试、K3s 环境传入各自地址。MySQL 用户名和密码应使用 Kubernetes Secret
注入，不能继续将 `root/root` 写在源码或镜像中。

### 2. 中间件在 K3s 集群外

在 `doit` 命名空间创建名称为 `doitedu` 的 Service 与 EndpointSlice，将端口指向
**实际运行中间件的 LAN IP**。不要把 Proxmox 宿主机地址默认当作 Kafka/MySQL 地址。

EndpointSlice 至少应包含 Kafka `9092` 和 MySQL `3306`，如作业还依赖 Redis、
ZooKeeper、Doris，则按实际依赖补充端口。Service/EndpointSlice 只解决入口地址；各服务
自身向客户端返回的地址也必须对 Pod 可达。

### 3. 中间件在 K3s 集群内

改用各组件的 Kubernetes Service DNS，不再依赖历史 hosts 名称。例如：

```text
<kafka-service>.doit.svc.cluster.local:9092
<mysql-service>.doit.svc.cluster.local:3306
```

服务名必须以实际已创建的 Service 为准。

### 4. Kafka 的额外检查

Kafka 的 `bootstrap.servers` 只用于首次获取元数据。Broker 返回的
`advertised.listeners` 中每一个地址，也必须能被 Flink Pod 解析和连接。否则 DNS 修好后，
客户端会在连接 Broker 的下一步继续失败。

## 临时手段与不建议的做法

可在 Flink Pod 模板中使用 `hostAliases`，将 `doitedu` 临时映射到一个已确认的内网 IP，
用于快速验证网络路径。它不适合长期维护：地址变更、多个环境和多个服务会使配置难以追踪。

不要通过关闭 Flink failover、无限增加重启次数或忽略日志来“解决”该问题；这些操作不能让
DNS 可解析，只会延长失败循环。

## 修复后的验收顺序

1. 从 TaskManager Pod 内完成 DNS 和端口连通性检查。
2. 验证 Kafka broker 的所有 advertised 地址均可达。
3. 验证 MySQL CDC 使用的账号、binlog 与表权限。
4. 重启或重新提交作业后，确认 `log-source`、CDC source 不再出现解析错误。
5. 观察一次以上成功 checkpoint，确认作业状态稳定且不再触发全局 failover。
