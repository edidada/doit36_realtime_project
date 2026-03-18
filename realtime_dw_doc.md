# Realtime_DW 模块分析

## 模块功能概述

realtime_dw是一个实时数据仓库模块，基于Apache Flink流处理框架，用于处理和分析实时用户行为数据。该模块包含以下主要功能：

### 1. 数据ETL（数据清洗和转换）
- **EtlJob01_UserEventsLogCommonDim**: 用户行为日志数据公共维度退维任务
- **EtlJob02_MallTfcAg01**: 商城流量聚合处理
- **EtlJob03_PageAccessTimeLong**: 页面访问时长统计
- **EtlJob04_InteractiveEventsOverview**: 交互事件概览分析
- **EtlJob05_VideoEventsAgg**: 视频事件聚合统计
- **EtlJob06_SearchEventsAnalyse**: 搜索事件分析
- **EtlJob07_AdRequestSessionProcess**: 广告请求会话处理

### 2. 数据同步
- **SyncJob01_UserInfo2Hbase**: 用户信息同步到HBase
- **SyncJob02_VideoInfo2Hbase**: 视频信息同步到HBase
- **SyncJob03_AdRequestFeatures2Hbase**: 广告请求特征同步到HBase

### 3. 实时看板
- **Promotion01_PVUV**: 营销活动PVUV实时监控

## 技术栈

- **流处理框架**: Apache Flink 1.16.1
- **状态管理**: EmbeddedRocksDBStateBackend
- **数据源**: Kafka
- **维度存储**: HBase
- **结果存储**: MySQL、Redis、Doris
- **CDC**: MySQL CDC 2.3.0
- **SQL支持**: Flink Table API & SQL

## Promotion01_PVUV 程序运行测试

### 程序功能
营销活动A/B/C的活动页（page006, page007, page008）流量实时监控指标计算：
- 计算每个营销活动页当天累计到当前的PV（页面浏览量）
- 计算每个营销活动页当天累计到当前的UV（独立访客数）
- 每分钟更新一次统计结果

### 本地测试版本运行结果

**任务名称**: 营销活动PVUV实时监控-本地测试版

**测试数据**:
- 9个页面加载事件，分布在3个不同的活动页面
- 用户ID范围: 1001-1009
- 活动页面: /page006, /page007, /page008

**运行日志分析**:

```
[INFO] Received JobGraph submission '营销活动PVUV实时监控-本地测试版'
```

**输入事件**:
```
EventBean(user_id=1001, device_type=android, release_channel=app, session_id=session001, page_type=page, page_url=/page006, event_id=page_load, event_time=1773848449812, properties={url=/page006}, ...)
EventBean(user_id=1002, device_type=ios, release_channel=app, session_id=session002, page_type=page, page_url=/page006, event_id=page_load, event_time=1773848449812, properties={url=/page006}, ...)
...
```

**PVUV统计结果**:
```
PVUVResult{pageUrl='/page006', updateTime=2026-03-18T23:41:03.678290, pv=1, uv=1001}
PVUVResult{pageUrl='/page006', updateTime=2026-03-18T23:41:03.689132, pv=1, uv=1002}
PVUVResult{pageUrl='/page007', updateTime=2026-03-18T23:41:03.689859, pv=1, uv=1003}
PVUVResult{pageUrl='/page007', updateTime=2026-03-18T23:41:03.690512, pv=1, uv=1004}
PVUVResult{pageUrl='/page008', updateTime=2026-03-18T23:41:03.691193, pv=1, uv=1005}
PVUVResult{pageUrl='/page008', updateTime=2026-03-18T23:41:03.691878, pv=1, uv=1006}
PVUVResult{pageUrl='/page006', updateTime=2026-03-18T23:41:03.692493, pv=1, uv=1007}
PVUVResult{pageUrl='/page007', updateTime=2026-03-18T23:41:03.693075, pv=1, uv=1008}
PVUVResult{pageUrl='/page008', updateTime=2026-03-18T23:41:03.693559, pv=1, uv=1009}
```

**统计汇总**:
- **/page006**: PV=3, UV=3 (用户: 1001, 1002, 1007)
- **/page007**: PV=3, UV=3 (用户: 1003, 1004, 1008)
- **/page008**: PV=3, UV=3 (用户: 1005, 1006, 1009)

### 程序特点

1. **实时计算**: 基于Flink流处理，实时计算PVUV指标
2. **窗口聚合**: 使用CUMULATE窗口进行累计统计
3. **维度关联**: 支持与HBase维表进行关联查询
4. **多存储支持**: 结果可写入MySQL、Redis等多种存储
5. **状态管理**: 使用RocksDB状态后端，支持大规模状态
6. **容错机制**: 支持Checkpointing，保证数据一致性

## 总结

realtime_dw模块是一个完整的实时数据仓库解决方案，涵盖了数据采集、清洗、转换、聚合、存储等全流程。该模块展示了Flink在企业级实时数据处理中的典型应用场景，包括：
- 实时ETL数据处理
- 维度数据关联
- 实时指标计算
- 多种数据源和存储的集成
- 状态管理和容错机制

Promotion01_PVUV程序成功演示了如何使用Flink进行实时流量监控，为营销活动提供实时数据支持。