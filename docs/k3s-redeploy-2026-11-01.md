# 合并修复与 K3s 发布记录（2026-11-01）

## 发布前发现与修复

基线为用户解决冲突后的 `k3s/a5f3bf3`。最初 `mvn package` 失败，历史 CI `41255487569` 也因相同 POM 错误失败。本次保留 K3s 的 RuntimeConfig、HDFS 客户端、注解处理器配置和业务 SQL，只修复合并遗留：

- `rt_rule_mk` 重复的 `<exclusions>` 块导致 Maven 无法解析。
- `realtime_dw`、`rt_rule_mk`、`redis_demo` 重新混入 Log4j2 2.17.2，与父 POM 2.26.1 重复。
- ETL 配置测试的 JUnit Jupiter 依赖被合并丢失；已补回，未通过跳过测试规避。
- 两个模块重复的 exclusion 条目去重；规则模块 Lombok 与父 POM 对齐，改为 provided。
- 增加 `scripts/check-maven-model.ps1` 并接入 Maven CI，防止重复 dependency/exclusions 再次合入。

本地 `mvn -B -ntp package`、`mvn -B -ntp clean package` 均成功，82 项测试无失败/跳过；六个模块 Log4j2 实际绑定/XML/落盘探针全部通过，支付/优惠券真实 HTTP 验收 14 项通过。

## 现场与保护措施

五节点 Ready；原工作负载是 `realtime-dw/realtime-dw-etl-01`，`RUNNING/STABLE`，Flink 1.16.1，旧包入口；中间件位于 `middleware`。Proxmox VM 107 已核实为 stopped，未启动它。未改动其他项目、RabbitMQ、Kafka/HBase/Doris 的工作负载或 PVC。

旧 JobID：`f308ea65eba4bb7b6a1a29305c0a3b49`。配置备份在 master `/var/tmp/realtime-dw-upgrade-20261101-0817/`，目录 0700、JSON/状态归档 0600；本地副本位于忽略的 `.artifacts/k3s-upgrade-20261101/`。未导出 Secret 明文，旧镜像与旧 ConfigMap 保留，不删除状态卷。

升级前 canonical savepoint：`file:/flink-data/savepoints/savepoint-f308ea-46d84ddae853`，同时将该目录归档到 master 的私有备份目录。该备份不表示迁移一定兼容；新版还必须验证实际恢复。

## 构建与预检

`scripts/prepare-k3s-runtime.ps1 -SkipBuild` 从本地已验证 classes/JAR 构建独立 bundle，带逐文件 SHA256；Windows 生成的清单采用 LF，避免 Linux 将 CR 当成文件名一部分。

本次发布包为 `realtime-dw-20261101-081853-6f1c1cb8`。在 worker3 上使用现有 Podman 构建，不在节点编译 Java、不启动额外构建 VM；构建 RUN 步骤限制 1 CPU/1GiB。基础镜像为 Flink 1.19.3/Java 17，拉取摘要 `sha256:2c54c237a9dea5fc679b51565ea7444508befbefee9fd7539fbdbff1820602e2`。

镜像 `docker.io/doit36/flink-jobs:20261101-6f1c1cb8`：配置 ID `9126bf953a34b0562e0debdf998de61c860f26ebf9ee50196963cded43058bf0`，导入 manifest 摘要 `93ff0a224fb1637672864729f448eff498fd6e872d889fc9a3b51d8d9756537e`。替换 Flink 自带的旧日志栈为 2.26.1，并移除 usrlib 的重复绑定/全量 planner；两个 Flink 模块和 Risk 可执行 JAR 随镜像提供。

镜像内两个模块的 LoggingProbe 及 Risk 8/8 公开回归样例通过。K3s 隔离预检 Job 先执行 Risk 基准，再执行 ETL 的 `flink info`（只生成计划，不启动消费）。预检不暴露支付服务。

## 发布与验收结果（已完成）

- 升级前真实链路：5 条隔离测试事件完成 HBase 补维，Kafka 输出 5、Doris 输出 5；checkpoint 283 -> 286，failed 保持 5（节点重启后的历史失败，不是本次新增）。
- 验收脚本使用不超过 20 字符的 session，匹配现网 Doris VARCHAR(20)；最初过长的测试 session 被 Doris 截断，只影响查询匹配，已修复脚本，未修改业务表结构。
- K3s 预检 Job `realtime-dw-preflight-20261101-6f1c1cb8-plan` Completed，Risk 8/8、`FLINK_PLAN_OK`。首次计划预检缺少 CLI 的依赖 classpath，修复为逐个 `-C` 后通过；首次失败 Job 保留至 TTL 清理，不隐去失败记录。
- Flink 从 1.16.1 升至 1.19.3，新入口 `com.test.bigdata.rtdw.data_etl.EtlJob01_UserEventsLogCommonDim`；最终 generation 7，`RUNNING/STABLE`，JM/TM 均 Ready、重启 0，实际镜像 ID 均为上述 `9126bf...`。
- 首次升级由 Operator 创建 COMPLETED 的 FlinkStateSnapshot，最终旧版 savepoint 为 `file:/flink-data/savepoints/savepoint-f308ea-90ba48ff5019`；新版 REST 显示 restored=1，明确从该路径恢复。旧版到新版的首次链路验证 Kafka/Doris 各 5 条通过。
- Operator 不接受 `logConfiguration.log4j2.xml`，已改为独立 immutable ConfigMap 挂载 XML。Flink 自带的旧 `log4j.configurationFile` 参数还会覆盖同名配置，改用优先级更高的 `log4j2.configurationFile`，并实际验证后执行第二次 savepoint 升级，没有无状态重启。
- 最终 JobID `0ded6b07d4bbcdf270d05b167cd55a39`，从 `file:/flink-data/savepoints/savepoint-0de220-24a6083b870f` 恢复；最终验收 session `u0bf01f2c81fd48cf9d`，输入 5，Kafka 输出 5、Doris 输出 5，HBase 用户/地理/页面维度均符合预期。
- 最终验收 checkpoint completed 220 -> 223，failed 4 -> 4；这 4 个失败在验收前已存在，当前链路验收无新增失败，不宣称全过程零失败。最终实例已持续运行约 19 分钟后再次验收。
- JM、TM 的 XML 日志均已落到 PVC 的 `/flink-data/logs/<pod-name>/flink-runtime.log`，核实大小分别为 253214、367086 字节；按 Pod 分目录，滚动压缩并清理超期归档。
- 发布只允许 savepoint 升级，`allowNonRestoredState=false`，禁止静默无状态回退。若恢复失败，保留现场并恢复旧版本，不自行丢弃状态或重置 offset。

最终配置、savepoint 与验收 JSON 另存 master `/var/tmp/realtime-dw-logging-20261101-0833/`，本地私有副本在 `.artifacts/k3s-upgrade-20261101/`。此次不修改、删除中间件业务数据，仅向现有业务 topic/table 添加隔离测试 session 的合成事件。

验收范围是当前正在部署的 ETL 主链路、镜像中 Risk 基准，以及本地单元/HTTP/日志测试；没有把仓库全部 main 函数都作为常驻服务部署，也未做压测、容灾切换或真实支付通道验收。K3s 工作负载 CPU/内存、节点绑定、Secret 引用与 PVC 保持不变。镜像目前仅缓存于 worker3，未推送公共仓库；换节点前必须先导入镜像，且 local-path PVC 也不能任意漂移。

当前分支 `k3s` 的修改尚未 commit/push；历史 Actions 失败已核实，但未提交的修复不能称为远程 CI 已通过。此次完成的是本地构建与内网实际部署验收。

## 可复用命令

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/check-maven-model.ps1
mvn -B -ntp clean package
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/prepare-k3s-runtime.ps1 -SkipBuild
```

在 master 上：

```bash
python3 scripts/k3s-stateful-upgrade.py prepare /var/tmp/<private-new-backup-directory>
python3 scripts/k3s-stateful-upgrade.py preflight /var/tmp/<backup-directory> --image <cached-image> --job-name <unique-job-name>
# 确认 Job 完成、VM107 stopped、镜像已导入 worker3 后：
python3 scripts/k3s-stateful-upgrade.py deploy /var/tmp/<backup-directory> --image <cached-image> --log-xml deploy/k3s/log4j2.xml
python3 scripts/k3s-stateful-upgrade.py status /var/tmp/<backup-directory>
python3 scripts/verify-k3s-business.py --require-version 1.19.3 --output /var/tmp/<new-report.json>
```

旧 `deploy/k3s/business/deploy-business.sh` 指向已删除的 `doit` 和旧小 JAR 热替换流程，不能继续照旧执行。新流程保留现网 Namespace、Secret 引用、资源、节点、PVC，仅更新目标 Flink 工作负载。

[Flink 版本升级说明](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/ops/upgrading/)与 [Operator 作业管理说明](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/managing/job-management/)作为操作依据；文档兼容性描述不替代本项目的实际 savepoint 恢复验收。
