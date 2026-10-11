# 2026 年实时数仓架构与 HBase 选型判断

## 结论

这套项目的核心思路——`Kafka + Flink + 明细/维表存储 + 实时 OLAP + Redis`——仍是一线实时数仓的主流范式；变化主要在组件选型和云化运维。整套课程式组合原样使用已经不常见，但各组件承担的职责仍然成立。

| 2023 课程组合 | 2026 常见演进 |
| --- | --- |
| Flink 1.16 | Flink 2.x；重点是云原生、状态管理与实时 AI 集成 |
| HBase 自建集群 | HBase、Lindorm、Bigtable、Cassandra/Scylla、云 KV，按场景选择 |
| HBase 做所有维表 | 热维度常用 Redis；超大、稀疏、主键查询维度才保留 HBase/宽表 KV |
| Doris | Doris / StarRocks / ClickHouse / Pinot 等实时 OLAP，按生态和运维能力选 |
| MySQL CDC + Kafka | 仍很常见，通常用 Debezium、Flink CDC 或云托管 CDC |
| Redis | 仍是缓存、实时画像、看板结果的主力组件 |

## HBase 是否因为性能问题被替换

HBase 没有因为“性能差”被淘汰。它的强项仍是 PB 级宽表、主键点查、高吞吐写入、稀疏列和时序/画像类数据。Apache 在 2026 年仍发布 HBase 3.0.0、2.6.6 等版本，并持续维护 JDK 17 兼容性。

- [Apache HBase 下载页](https://hbase.apache.org/downloads/)
- [Apache HBase JDK 支持说明](https://hbase.apache.org/book.html)

国内头部场景里更常见的是“被兼容型替代”，不是概念被抛弃。例如阿里云说明，其 Lindorm 宽表引擎为淘宝、支付宝、菜鸟、优酷、高德等核心业务提供支撑，并保持 HBase 客户端/API 兼容；这本质上是保留 HBase 的数据模型，但使用更云原生、索引更强、运维更轻的实现。

- [阿里云 HBase 性能增强版说明](https://www.alibabacloud.com/help/en/hbase/user-guide/performance-enhanced-edition-overview)

国外也是相近趋势：AWS EMR 当前仍提供 HBase 2.6.x；Google Cloud Bigtable 保持 HBase API 兼容，方便存量 HBase 应用迁移，但建议新 Java 项目优先使用原生 Bigtable 客户端。

- [AWS EMR HBase](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hbase.html)
- [Google Bigtable HBase API](https://docs.cloud.google.com/bigtable/docs/hbase-bigtable)

## 为什么会替换 HBase

替换原因通常不只是性能。

- **自建运维重**：涉及 HDFS、RegionServer、WAL、Compaction、热点拆分、ZooKeeper、备份和跨机房容灾。
- **查询能力弱**：HBase 擅长按 RowKey 查；二级索引、复杂过滤、全文检索和 OLAP 查询不是原生强项。
- **云环境偏好存算分离**：弹性扩缩和托管服务降低了长期运维成本。
- **规模不足时不划算**：几百万到几千万维表数据，用 MySQL + Redis、云 KV 或 OLAP 查询通常更简单。
- **业务模型不同**：需要多模型、SQL 或搜索时，Lindorm、Bigtable、Cassandra/Scylla、MongoDB、Doris/ClickHouse 等产品可能更贴合。

## 对本项目的建议

- **用户/视频维表**：教学规模或中小规模场景未必值得上 HBase；可以考虑 MySQL CDC 后写 Redis，或直接做 Flink Lookup Join。
- **超大用户画像、广告特征、稀疏标签**：HBase/Lindorm/Bigtable 仍然合理。
- **宽表聚合、看板查询**：继续放 Doris 是合理的，不要拿 HBase 替代实时 OLAP。
- **Flink 状态**：继续使用 RocksDB 或托管状态后端，不要把 HBase 当 Flink 状态库。
- **版本升级**：项目使用的 Flink 1.16 已偏旧；若计划长期维护，应规划迁移到 Flink 2.x，而不仅是将 JDK 改为 17。

Apache Flink 2.x 仍在持续演进：

- [Apache Flink 下载页](https://flink.apache.org/downloads/)

## 一句话总结

HBase 没死，也不是“慢”；它从“默认大数据数据库”变成了“适合特定高吞吐宽表场景的专业选项”。新项目更常优先选择托管、云原生且更贴业务模型的替代品。
