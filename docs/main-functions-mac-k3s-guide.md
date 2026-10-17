# main 功能、Mac 测试、k3s 部署与排错指南

本文根据仓库源码、脚本及 2026-10-17 已保存的验收记录整理。地址和集群状态是该日期的环境记录，重部署前应重新检查。当前共 42 个 Java main（包括 src/test/java），另有 1 个 Scala main。

## 1. 每个 main 做什么

表中 Java 类名可直接传给 `bash scripts/java-mains.sh <完整类名>`。下列包前缀用于拼接完整类名；脚本的 `--commands` 会打印全部完整命令。

### 实时数仓：前缀 `cn.doitedu.rtdw.`

| 类名（接在前缀后） | 功能及测试前提 |
| --- | --- |
| data_etl.EtlJob01_UserEventsLogCommonDim | Kafka 行为日志关联 HBase 用户、地域、页面等公共维度，宽表写入 Kafka 和 Doris；先准备维表和目标表。 |
| data_etl.EtlJob02_MallTfcAg01 | 商城流量轻度聚合，读取 Kafka 数据并写 Doris；需有符合字段约定的输入。 |
| data_etl.EtlJob03_PageAccessTimeLong | 计算页面访问停留时长，输出 Doris 页面时长统计；需要同一访问过程的连续事件。 |
| data_etl.EtlJob04_InteractiveEventsOverview | 聚合点赞、收藏、分享等互动指标，写入 Doris。 |
| data_etl.EtlJob05_VideoEventsAgg | 聚合视频播放和互动事件，关联 HBase 视频维度，写入 Doris。 |
| data_etl.EtlJob06_SearchEventsAnalyse | 同步请求相似词 HTTP 服务，扩展搜索词并计算搜索指标；需真实 SIMWORDS_URL 服务。 |
| data_etl.EtlJob06_SearchEventsAnalyse_Async | 搜索分析的异步 HTTP I/O 版本；无搜索事件时启动成功不能证明接口可用。 |
| data_etl.EtlJob07_AdRequestSessionProcess | 处理广告请求会话与特征，使用 Kafka、HBase、Doris。 |
| data_sync.SyncJob01_UserInfo2Hbase | MySQL CDC 会员信息同步到 HBase 用户维表。 |
| data_sync.SyncJob02_VideoInfo2Hbase | MySQL CDC 视频信息同步到 HBase 视频维表。 |
| data_sync.SyncJob03_AdRequestFeatures2Hbase | Kafka 广告请求事件同步到 HBase 特征表。 |
| deployment.StandaloneEventsJob | datagen → SQL → 文件输出，用于验证 Flink 基础设施；设置可写的 STANDALONE_OUTPUT_URI。 |
| rt_dashboard.Promotion01_PVUV | 推广流量 PV/UV 看板，依赖 Kafka、MySQL 和 Redis。 |
| rt_dashboard.Promotion01_PVUVLocal | 有限输入的 PV/UV 本地演示；等待自然退出，检查结果。 |
| utils.DimRecProcess | CSV 转 HBase shell put 命令，输入和输出文件可用两个命令行参数指定；本身不执行这些 put。 |
| utils.HttpRequestDemo | 相似词 HTTP POST 最小示例；需要真实接口。 |

### 数仓实验：前缀 `cn.doitedu.`

| 类名 | 功能及测试前提 |
| --- | --- |
| EtlAsync | Socket 文本输入 + 异步相似词请求；必须同时准备 Socket 和 HTTP 服务。 |
| Http | 相似词 HTTP 请求实验；需要真实接口。 |
| WindowTest | Kafka、窗口和 checkpoint 实验；Mac 覆盖默认 HDFS 路径，使用可写 file URI。 |

### 规则引擎：前缀 `cn.doitedu.rtmk.`

每一代都保留原入口和 Local 有限输入入口。Local 表示输入方式适合演示，不能推断它完全不访问 HBase、Redis 等外部系统。具体依赖还取决于选用的规则计算器。

| 类名 | 功能及测试重点 |
| --- | --- |
| demo1.TargetProductCartMonitor | 指定商品加购物车监控，展示固定规则；Kafka 输入。 |
| demo1.TargetProductCartMonitorLocal | 有限事件验证指定商品购物车规则。 |
| demo2.MultiRuleEngine | 同一事件流执行多条规则。 |
| demo2.MultiRuleEngineLocal | 有限事件验证多规则命中。 |
| demo3.RuleCalculatorPattern | 使用计算器接口和实现类拆分规则逻辑。 |
| demo3.RuleCalculatorPatternLocal | 有限事件验证计算器模式。 |
| demo4.ModelBasedRuleEngine | 按规则模型及参数创建计算器。 |
| demo4.ModelBasedRuleEngineLocal | 有限事件验证模型化规则。 |
| demo5.BitmapOptimizedRuleEngine | 使用 Bitmap 优化人群判断等规则处理。 |
| demo5.BitmapOptimizedRuleEngineLocal | 有限事件验证 Bitmap 规则。 |
| demo6.DynamicRuleManagement | MySQL CDC 规则元数据与 Kafka 事件连接，动态上线、变更、下线规则。 |
| demo6.DynamicRuleManagementLocal | 有限事件演示动态管理；规则模型需与 demo6 计算器匹配。 |
| demo7.DynamicRuleManagementV2 | 第二版动态管理，扩展规则模型及处理方式。 |
| demo7.DynamicRuleManagementV2Local | 有限输入验证第二版动态规则。 |
| demo8.RuleEngineRecovery | 广播规则元数据与 keyed 用户状态持久化，支持 FLINK_SAVEPOINT_PATH 恢复；依赖 Kafka、MySQL CDC，计算器可使用 Redis/HBase。 |
| demo8.RuleEngineRecoveryLocal | 有限输入验证恢复版本的规则逻辑；单次运行结束不等于已验证跨进程恢复。 |
| demo9.DynamicRuleWithGroovy | 从规则元数据加载 Groovy 计算器，实现动态脚本规则。 |
| demo9.DynamicRuleWithGroovyLocal | 有限输入验证 Groovy 动态规则。 |
| demo9.groovy.TestDiaoyong | 读取指定 Groovy 文件，编译并调用；第一个参数为文件路径。 |

### 数据准备与 Redis 示例

| 完整类名 | 功能及测试前提 |
| --- | --- |
| cn.doitedu.test.EventsMoni | 向 Kafka 写模拟事件；EVENT_BATCHES=1 可限制批次，KAFKA_OUTPUT_TOPIC 指定测试 topic。 |
| cn.doitedu.test.HbaseProfileTagsMoni | 向 HBase 写合成用户画像标签；会修改外部数据。 |
| cn.doitedu.test.HistoryConditionValueMoni | 向 Redis 写历史条件值；选择隔离测试 DB。 |
| cn.doitedu.jedis.RedisDemo | Jedis 连接与 Redis 操作演示；会访问 Redis。 |

Scala 入口 `cn.doitedu.batch.jobs.DataLoadJob01_GeoAreaInfo2Hbase` 使用本地 Spark，从 MySQL 的 t_md_areas 读取地域数据、计算 geohash 后写 HBase dim_geo_area。它不在 Java 42 项启动目录中，可使用 `bash scripts/run-main.sh batch_jobs cn.doitedu.batch.jobs.DataLoadJob01_GeoAreaInfo2Hbase`；需事先导出 MySQL/HBase 环境变量。

## 2. 在 Mac 上测试

### 2.1 编译与启动工具

安装 JDK 17、Maven、Python 3，在仓库根目录运行：

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"
java -version
mvn -version
python3 --version
bash scripts/java-mains.sh --prepare
bash scripts/java-mains.sh --list
bash scripts/java-mains.sh --commands
```

`--prepare` 只编译并生成依赖 classpath，不启动 Kafka/MySQL、不修复 DNS，也不创建业务数据。脚本会应用 `scripts/java17.options`；在 IDEA 中启动时也应配置该参数文件，并把 working directory 设为项目根目录。

### 2.2 配置连接

Java 启动脚本默认加载本地 `.artifacts/main-audit/k3s/runtime-env.json`（存在时），也支持 `MAIN_ENV_JSON` 显式选择配置。该文件被 Git 忽略，新 clone 不会自带。JSON 为环境变量名到字符串值的对象；脚本加载时文件同名值覆盖终端变量。不要把实际密码写入文档。

没有该文件时，按程序依赖导出配置，例如：

```bash
export KAFKA_BOOTSTRAP_SERVERS=192.168.31.211:9092
export HBASE_ZOOKEEPER_QUORUM=192.168.31.211:2181
export MYSQL_HOST=127.0.0.1 MYSQL_PORT=13306
export MYSQL_USERNAME='<测试账号>' MYSQL_PASSWORD='<测试密码>'
export MYSQL_RULE_DATABASE=doit36_main_audit
export REDIS_HOST=127.0.0.1 REDIS_PORT=16379 REDIS_DATABASE=15
export REDIS_PASSWORD='<实际密码；无密码时按环境配置>'
export DORIS_FENODES=192.168.31.251:8030
export DORIS_USERNAME='<测试账号>' DORIS_PASSWORD='<测试密码>'
```

MYSQL_DATABASE 为业务库，MYSQL_RULE_DATABASE 为规则库。库表、binlog 权限、画像和规则元数据都需要提前准备。初始化文件见 [业务部署包](../deploy/k3s/business/README.md)、`deploy/k3s/business/init-main-validation.sql` 和 `init-rule-validation.sql`；先检查目标库与覆盖范围，再执行。demo6 的规则模型与后续版本不同，全量执行器为它选择独立的规则测试库。

Mac 不能直接把集群 ClusterIP 当 LAN 地址访问。当前 MySQL、Redis 通过另一终端保持 SSH 隧道：

```bash
ssh -N -o ExitOnForwardFailure=yes \
  -L 13306:10.43.251.207:3306 \
  -L 16379:10.43.215.114:6379 root@192.168.31.77
```

以上 Service IP 是当时环境值，先用集群 `kubectl get svc -A` 核对。若本地端口占用，先检查是否已有隧道。Kafka advertised.listeners、HBase 在 ZooKeeper 中登记的 RegionServer 地址、Doris FE 返回的 BE 地址也必须对 Mac 可达。

### 2.3 从小例子到真实流任务

```bash
mkdir -p .artifacts
bash scripts/java-mains.sh cn.doitedu.rtdw.utils.DimRecProcess \
  examples/main-inputs/dim_rec.csv .artifacts/dim-rec-commands.sh
bash scripts/java-mains.sh cn.doitedu.rtmk.demo9.groovy.TestDiaoyong \
  examples/main-inputs/hello.groovy

# 准备连接、规则和画像后，先运行消费者
KAFKA_INPUT_TOPIC=main-audit-rules KAFKA_GROUP_ID=manual-rule-check \
  bash scripts/java-mains.sh cn.doitedu.rtmk.demo8.RuleEngineRecovery

# 另一终端写入测试事件；注意私有 JSON 可能覆盖同名变量
EVENT_BATCHES=1 KAFKA_OUTPUT_TOPIC=main-audit-rules \
  bash scripts/java-mains.sh cn.doitedu.test.EventsMoni
```

ETL 测试顺序：准备 MySQL/HBase/Doris 表 → 维表同步 → ETL01 宽表 → 各聚合或看板 → 发送并核对事件。使用独立 topic、consumer group、目标表和 Redis DB，避免影响已有业务。

StandaloneEventsJob 设置 `STANDALONE_OUTPUT_URI="file://$PWD/.artifacts/standalone-output"`；WindowTest 设置 `FLINK_CHECKPOINT_URI="file://$PWD/.artifacts/window-checkpoints"`。HTTP 示例需要 SIMWORDS_URL；EtlAsync 还需要 SOCKET_HOST 和 SOCKET_PORT。现有验收环境缺这两个服务，换 IP 或增加 JVM 参数不会自动补齐。

### 2.4 日志、全量执行和成功标准

三个模块的 `src/main/resources/log4j2.xml` 同时配置控制台及 RollingFile。脚本启动时按完整类名输出 `logs/runtime/<类名>.log`，单文件 20 MB 滚动，最多保留 3 个压缩归档。保留并提交日志前检查密码、token 和业务数据。

```bash
tail -f logs/runtime/cn.doitedu.rtmk.demo8.RuleEngineRecovery.log
python3 scripts/execute-java-mains.py --seconds 90
```

全量执行会真实访问并可能写入测试系统，不是纯单元测试。有限任务等待退出 0；无界流观察两个成功 checkpoint 或达到观察时限后由脚本停止。退出 143 表示 SIGTERM，Ctrl+C 通常为 130，二者都不能称为自然结束成功。

已保存的最近一次 [42 项日志与结果](../logs/validation/README.md)：16 项自然退出 0，23 项流任务完成至少两个 checkpoint 后主动停止，3 项因缺少 HTTP/Socket 服务未通过。流任务 RUNNING/checkpoint 不等于所有业务分支均验证；ETL01 的 Kafka/Doris 双输出和 RuleEngineRecovery 的 savepoint 恢复另见 [真实业务验收](k3s-main-execution.md)。

恢复验证应先产生可核对的用户状态，执行 stop-with-savepoint，再以 FLINK_SAVEPOINT_PATH 启动新进程，只发送触发事件，检查旧状态仍生效。仅重启空任务不足以证明恢复成功。

## 3. k3s 是如何部署的

### 3.1 已有环境与版本边界

| 节点 | 地址 | 本项目部署用途 |
| --- | --- | --- |
| master | 192.168.31.77 | 控制平面、部署入口；MySQL/Redis 隧道跳板 |
| worker1 | 192.168.31.211 | Kafka 9092；HBase/ZooKeeper 2181，RegionServer 16020 |
| worker2 | 192.168.31.251 | Doris FE 8030/9030、BE 8040/9050 |
| worker3 | 192.168.31.88 | Flink；Dashboard NodePort 30816 |

项目使用独立 `doit` 命名空间；MySQL、Redis 复用现有 middleware 服务。部署包使用单副本、节点绑定及 local-path PVC；不是多副本高可用方案。中间件使用 hostNetwork 通告可达 LAN 地址。已部署业务镜像仍为 Flink 1.16.1；当前源码和 Mac 验证使用 Flink 1.19.3，不能将 Mac 编译成功当成集群镜像已升级。

### 3.2 部署流程与脚本边界

1. 核对 SSH、节点 Ready、Flink Kubernetes Operator、镜像缓存和 PVC；原有业务资源不能随意替换。
2. **新建、恢复或扩容 Flink 前，PVE VM 107 必须已 stopped**，为集群释放宿主机资源。
3. 在需要时准备 Kafka/HBase/Doris 运行目录、镜像与中间件清单。
4. 构建/导入匹配版本的作业镜像，注入环境配置和 Secret，部署 FlinkDeployment。
5. 检查全部算子、checkpoint，再发送唯一测试批次，核对 Kafka、HBase 维度和 Doris 行数。

```bash
# 只读核对
ssh root@192.168.31.77 'kubectl get nodes -o wide'
ssh root@192.168.31.77 'kubectl -n doit get pods,svc,pvc'
ssh -i ~/.ssh/id_ed25519_pve root@192.168.31.171 'qm status 107'

# 部署门禁：非 stopped 返回失败；不会替你关闭 VM
ssh -i ~/.ssh/id_ed25519_pve root@192.168.31.171 \
  'test "$(qm status 107)" = "status: stopped"'

# 以下会修改部署，应先阅读业务部署包，并确认基础镜像/安装目录齐备
bash deploy/k3s/business/prepare-middleware.sh
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
bash deploy/k3s/business/deploy-business.sh
```

`deploy-business.sh` 默认通过 SSH 主机别名 k3s-master 连接；没有该别名时设置 `MASTER=root@192.168.31.77`。脚本编译 ETL01 的补丁类、小 JAR 以摘要命名的 ConfigMap 挂载，复用已有基础镜像依赖，初始化数据并调用 `verify-business.py`。这是已有镜像的补丁发布路径；源码依赖版本发生升级时，应先构建完整匹配的新镜像，不能默认小 JAR 已完成 Flink 升级。

完整镜像流程及导入方式见 [部署包说明](../deploy/k3s/README.md) 和 [部署详解](k3s-deployment.md)。模板 `flink-deployment.yaml.tpl` 需要替换镜像 tag 和入口；模板节点安排与当前业务清单可能不同，以实际应用的 YAML 为准。`deploy-standalone.sh` 会把同名业务作业切换为 datagen 验收作业，不要把它当成无影响的健康检查。

原业务验收使用唯一 session 发送 5 条事件，核对 Kafka 输出 5 条、Doris 5 行、HBase 维度一致，以及 checkpoint 成功数增加且没有新增失败。记录见 [业务部署包验收](../deploy/k3s/business/README.md)。本次编写文档没有重新发布集群或改变运行中的业务。

## 4. 此前排错思路与结论

| 现象/错误 | 判断与处理 |
| --- | --- |
| UnknownHostException: doitedu；No resolvable bootstrap urls | 先看异常链最底部。Mac 无法解析课程主机名，Kafka 和 MySQL 同时失败；改为运行环境配置与可达地址。 |
| --prepare 成功，执行仍报同样错误 | 编译与网络是两件事。检查实际启动日志中的 bootstrap.servers、hostname、port，确认配置被加载；脚本增加私有 JSON 自动加载。 |
| 加了 java17.options 仍 DNS 失败 | add-opens 解决模块反射访问，不能改变 DNS、端口或服务状态。 |
| 入口端口通，后续 Kafka/HBase/Doris 连接失败 | 检查协议返回的 broker、RegionServer、BE 地址；入口可达不代表所有后续端点可达。 |
| Mac 连不上集群 MySQL/Redis ClusterIP | 使用 SSH 本地转发；确认隧道仍运行、本地端口未冲突、目标 Service IP 正确。 |
| RuleEngineRecovery checkpoint 出 Kryo/InaccessibleObjectException | 广播状态曾保存带 Jedis 和 ValueState 句柄的计算器。修为只存规则元数据，在进程内缓存/重建计算器；继续增加 add-opens 不能修正错误状态模型。旧状态不能直接认定兼容。 |
| Groovy 动态规则或恢复异常 | 将脚本/元数据与运行时对象分开，检查动态计算器重建、规则独立状态名，并执行真实事件及 savepoint 恢复验证。 |
| GenericType/POJO getter/setter INFO | 属于类型提取提示；不能把它直接当作本次 DNS 故障根因。 |
| server-time-zone WARN | 根据 MySQL 实际时区配置 CDC，避免时间字段偏移；该警告不是 UnknownHostException 的原因。 |
| HTTP Connection refused、Socket 重启 | 服务未部署/未监听；需提供真实服务，不能宣称替换地址后业务就已通过。 |
| RUNNING 后不断 RESTARTING | RUNNING 可能只是短暂状态。向前找第一次异常和最底部 Caused by，修复后持续观察 checkpoint。 |
| exit 130 / 143 | 分别通常来自人工中断 / 检查器 SIGTERM；结合停止原因解释，不当成业务异常或正常退出 0。 |
| 控制台太长、找不到首次错误 | Log4j2 同时落盘，按类名查日志；保留原始失败和补跑证据，不能只保留最后一段 RUNNING。 |

排查顺序：确认运行位置（Mac JVM 或 Pod）→ 实际生效配置 → 主机解析 → TCP 连通 → 协议返回地址 → 身份权限/binlog/表结构 → 序列化与状态 → 数据结果与恢复。只读诊断应在实际出错的环境执行。

Mac 可先用 `dscacheutil -q host -a name <主机名>` 检查系统解析，再用 `nc -vz <主机名> <端口>` 检查 TCP；Pod 内使用可用的 getent/nc 工具。敏感配置只核对必要的地址和端口，不把完整环境变量、凭据 JSON 或 Secret 打印到共享日志。

## 5. 相关资料

- [统一 Java 启动入口](../scripts/java-mains.sh)
- [日志配置与执行说明](../logs/README.md)
- [最近一次 42 项执行结果](../logs/validation/README.md)
- [合并、业务读写及恢复验收](k3s-main-execution.md)
- [最初 DNS 故障记录](rule-engine-recovery-dns-troubleshooting.md)（描述的是修复前代码）
- [k3s 业务部署脚本与验收](../deploy/k3s/business/README.md)

旧文档中的 41/42/43 数量、默认 doitedu 地址和版本说明代表不同阶段；本文以当前脚本的 42 个 Java 入口及单独的 Scala 入口划分范围，历史记录应结合日期阅读。
