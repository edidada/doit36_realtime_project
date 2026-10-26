# realtime-dw
多易教育实时数仓36期项目课代码
## 1. 项目介绍

本项目是多易大数据的Java实时项目，主要包含以下模块：

- **realtime_dw**: 实时数仓模块，基于Flink进行实时数据处理和ETL
- **batch_jobs**: 批处理任务模块，基于Spark进行离线数据处理
- **redis_demo**: Redis操作示例模块
- **rt_rule_mk**: 实时规则引擎模块，基于Flink实现动态规则匹配
- **fin-olap-engine**: 单机 CSV/Parquet 金融交易报表、精确 SUM/COUNT 和 CSV 导出
- **fin-risk-engine**: 欧式期权 Black-Scholes、Greeks、可复现 Monte Carlo 与历史 VaR/ES

新增模块的使用方法、测试证据和设计完成度见 [金融计算模块交付说明](docs/finance-modules.md)，设计基线为 [maven_module.md](docs/design/maven_module.md)。两个模块可在 Win11/JDK 17 上独立测试，不依赖现有 K3s 中间件。

项目主要功能包括：
- 实时用户行为日志处理和维度打宽
- 实时流量概况多维模型统计
- 页面访问时长分析
- 搜索事件分析
- 视频事件聚合
- 广告请求实时特征工程
- 实时规则匹配和用户画像
- 数据同步（MySQL CDC、HBase等）

## 2. 项目环境

### 开发环境要求
- **Java**: JDK 17（根 POM 已固定 `maven.compiler.source/target` 为 17）
- **Maven**: 3.6.1+
- **Scala**: 2.12.12 (用于batch_jobs模块)

### 运行时依赖环境
- **Kafka**: 消息队列，用于实时数据传输
- **MySQL**: 数据库，用于存储业务数据和规则元数据
- **HBase**: NoSQL数据库，用于存储用户画像和标签
- **Redis**: 缓存数据库
- **Doris**: OLAP数据库，用于实时数据分析
- **Hadoop**: 分布式存储和计算框架

## 3. 项目依赖

### 主要依赖版本

#### Flink相关 (realtime_dw, rt_rule_mk)
- Flink: 1.16.1
- Flink Connector Kafka: 1.16.1
- Flink Connector HBase: 1.16.1
- Flink Connector JDBC: 1.16.1
- Flink StateBackend RocksDB: 1.16.1
- Flink CEP: 1.16.1
- Flink Table Planner: 1.16.1

#### Spark相关 (batch_jobs)
- Spark: 3.1.2
- Spark SQL: 3.1.2
- Spark Hive: 3.1.2
- Scala: 2.12.12

#### 数据库连接
- MySQL Connector: 8.0.30
- HBase Client: 2.4.9
- Redis Jedis: 4.3.1
- Elasticsearch: 7.17.5

#### 工具库
- Lombok: 1.18.24
- FastJSON: 1.2.83
- Geohash: 1.4.0
- RoaringBitmap: 0.9.39
- Groovy: 3.0.12
- HttpClient: 4.5.14

#### CDC相关
- Flink Connector MySQL CDC: 2.3.0
- Flink Doris Connector: 1.1.0

#### 日志
- SLF4J Log4j12: 1.7.30
- Log4j: 1.2.16

#### 测试
- JUnit: 4.13.1

## 4. 项目配置

### Maven配置
项目采用多模块Maven结构，父POM定义了公共配置：

```xml
<groupId>cn.doitedu</groupId>
<artifactId>realtime-dw</artifactId>
<version>1.0-SNAPSHOT</version>
<packaging>pom</packaging>
```

子模块：
- realtime_dw
- batch_jobs
- redis_demo
- rt_rule_mk

### 日志配置
使用Log4j进行日志管理，配置文件位于：
- `realtime_dw/src/main/resources/log4j.properties`
- `rt_rule_mk/src/main/resources/log4j.properties`

日志级别：INFO
输出格式：`[%-5p] %d --> %m %x %n`

### Flink配置
- 并行度：默认为1
- Checkpoint间隔：2000-5000ms
- Checkpoint模式：EXACTLY_ONCE
- State Backend：HashMapStateBackend / EmbeddedRocksDBStateBackend
- Checkpoint存储：`file:/d:/ckpt` 或 `file:/d:/checkpoint`

### Kafka配置
- Bootstrap Servers: `doitedu:9092`
- Topics: `dwd-events-log` (行为日志明细宽表)
- 消费组：动态生成

### 数据库配置
- MySQL: `doitedu:3306`, 用户名: `root`, 密码: `root`, 数据库: `realtimedw`
- HBase: `doitedu:2181` (ZooKeeper)
- Redis: `doitedu:6379`

## 5. 项目运行

### 编译项目
```bash
# 清理并编译所有模块
mvn clean compile

# 打包项目
mvn clean package

# 跳过测试打包
mvn clean package -DskipTests
```

### 运行Flink作业
#### realtime_dw模块
```bash
# 运行用户行为日志维度打宽作业
java -cp realtime_dw/target/realtime_dw-1.0-SNAPSHOT.jar cn.doitedu.rtdw.data_etl.EtlJob01_UserEventsLogCommonDim

# 运行实时流量概况多维模型统计
java -cp realtime_dw/target/realtime_dw-1.0-SNAPSHOT.jar cn.doitedu.rtdw.data_etl.EtlJob02_MallTfcAg01

# 运行页面访问时长分析
java -cp realtime_dw/target/realtime_dw-1.0-SNAPSHOT.jar cn.doitedu.rtdw.data_etl.EtlJob03_PageAccessTimeLong

# 运行搜索事件分析
java -cp realtime_dw/target/realtime_dw-1.0-SNAPSHOT.jar cn.doitedu.rtdw.data_etl.EtlJob06_SearchEventsAnalyse

# 运行视频事件聚合
java -cp realtime_dw/target/realtime_dw-1.0-SNAPSHOT.jar cn.doitedu.rtdw.data_etl.EtlJob05_VideoEventsAgg

# 运行广告请求实时特征工程
java -cp realtime_dw/target/realtime_dw-1.0-SNAPSHOT.jar cn.doitedu.rtdw.data_etl.EtlJob07_AdRequestSessionProcess
```

#### rt_rule_mk模块
```bash
# 运行实时规则引擎Demo
java -cp rt_rule_mk/target/rt_rule_mk-1.0-SNAPSHOT.jar cn.doitedu.rtmk.demo1.Demo1
java -cp rt_rule_mk/target/rt_rule_mk-1.0-SNAPSHOT.jar cn.doitedu.rtmk.demo2.MultiRuleEngine
java -cp rt_rule_mk/target/rt_rule_mk-1.0-SNAPSHOT.jar cn.doitedu.rtmk.demo3.Demo3
java -cp rt_rule_mk/target/rt_rule_mk-1.0-SNAPSHOT.jar cn.doitedu.rtmk.demo4.Demo4
java -cp rt_rule_mk/target/rt_rule_mk-1.0-SNAPSHOT.jar cn.doitedu.rtmk.demo5.Demo5
java -cp rt_rule_mk/target/rt_rule_mk-1.0-SNAPSHOT.jar cn.doitedu.rtmk.demo6.Demo6
java -cp rt_rule_mk/target/rt_rule_mk-1.0-SNAPSHOT.jar cn.doitedu.rtmk.demo7.Demo7
java -cp rt_rule_mk/target/rt_rule_mk-1.0-SNAPSHOT.jar cn.doitedu.rtmk.demo8.Demo8
java -cp rt_rule_mk/target/rt_rule_mk-1.0-SNAPSHOT.jar cn.doitedu.rtmk.demo9.Demo9
```

#### batch_jobs模块
```bash
# 运行批处理作业
java -cp batch_jobs/target/batch_jobs-1.0-SNAPSHOT.jar cn.doitedu.batch.jobs.DataLoadJob01_GeoAreaInfo2Hbase
```

#### redis_demo模块
```bash
# 运行Redis示例
java -cp redis_demo/target/redis_demo-1.0-SNAPSHOT.jar cn.doitedu.jedis.RedisDemo
```

### Flink集群运行
```bash
# 提交作业到Flink集群
flink run -c cn.doitedu.rtdw.data_etl.EtlJob01_UserEventsLogCommonDim realtime_dw/target/realtime_dw-1.0-SNAPSHOT.jar

# 提交作业并指定并行度
flink run -p 4 -c cn.doitedu.rtdw.data_etl.EtlJob01_UserEventsLogCommonDim realtime_dw/target/realtime_dw-1.0-SNAPSHOT.jar
```

## 6. 项目测试

### 单元测试
```bash
# 运行所有测试
mvn test

# 运行特定模块的测试
mvn test -pl rt_rule_mk

# 运行特定测试类
mvn test -Dtest=TestClassName
```

### 测试数据
项目包含多个测试数据文件：
- `model1_test.json`: 规则模型1测试数据
- `model2_test.json`: 规则模型2测试数据
- `model3_test.json`: 规则模型3测试数据
- `model4_test.json`: 规则模型4测试数据
- `model5_test.json`: 规则模型5测试数据
- `model5_test2.json`: 规则模型5测试数据2
- `x.json`: 其他测试数据

### 测试类
- `cn.doitedu.test.EventsMoni`: 事件监控测试
- `cn.doitedu.test.HbaseProfileTagsMoni`: HBase用户画像标签测试
- `cn.doitedu.test.HistoryConditionValueMoni`: 历史条件值测试
- `cn.doitedu.test.MysqlRuleMetaMoni`: MySQL规则元数据测试

### 监控和调试
- Flink Web UI: `http://localhost:8081` (默认端口)
- 日志输出: 控制台输出，格式为Log4j配置的格式
- Checkpoint监控: 可通过Flink Web UI查看Checkpoint状态

### 注意事项
1. 确保所有依赖服务（Kafka、MySQL、HBase、Redis等）已启动
2. 检查配置文件中的连接地址和端口是否正确
3. 首次运行前需要创建必要的数据库表和Kafka topics
4. 建议先在本地模式测试，确认无误后再提交到集群
5. 注意Checkpoint存储路径的配置，确保有足够的磁盘空间
6. 生产环境建议使用RocksDB State Backend以获得更好的性能

## 项目结构
```
realtime-dw/
├── realtime_dw/              # 实时数仓模块
│   ├── src/main/java/cn/doitedu/rtdw/
│   │   ├── beans/           # 数据Bean定义
│   │   ├── data_etl/        # ETL作业
│   │   ├── data_sync/       # 数据同步作业
│   │   ├── rt_dashboard/    # 实时看板
│   │   └── utils/           # 工具类
│   └── pom.xml
├── batch_jobs/              # 批处理模块
│   ├── src/main/java/cn/doitedu/batch/jobs/
│   └── pom.xml
├── redis_demo/              # Redis示例模块
│   ├── src/main/java/cn/doitedu/jedis/
│   └── pom.xml
├── rt_rule_mk/              # 实时规则引擎模块
│   ├── src/main/java/cn/doitedu/rtmk/
│   │   ├── beans/           # 规则Bean
│   │   ├── demo1-9/         # 各版本演示
│   │   └── groovy/          # Groovy规则脚本
│   └── pom.xml
├── lib/                     # 第三方jar包
│   └── flink-shaded-hadoop-3-uber-3.1.1.7.2.9.0-173-9.0.jar
├── pom.xml                  # 父POM
└── README.md                # 项目说明文档
```

## 技术栈总结
- **实时计算**: Apache Flink 1.16.1
- **批处理**: Apache Spark 3.1.2
- **消息队列**: Apache Kafka
- **数据库**: MySQL, HBase, Redis, Doris
- **搜索引擎**: Elasticsearch 7.17.5
- **编程语言**: Java 8+, Scala 2.12.12
- **构建工具**: Maven 3.6.1
- **日志**: Log4j + SLF4J
- **其他**: Groovy 3.0.12, RoaringBitmap, Geohash
