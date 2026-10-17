# 本轮 Java main 执行记录

2026-10-17，全部 **42 个 Java main** 已执行（含测试目录，不含 Scala）。
完整输出见每项日志，机器可读结果见 [results.json](results.json)。

流任务完成两个 checkpoint 或达到 90 秒后主动停止；退出码 143 是检查脚本发送 SIGTERM，
不代表正常退出，也不等同完整业务验收。RUNNING 仅表示日志曾出现该状态，应结合异常判断。
有限输入任务等待自然结束；所有 16 项最终均退出 0。

HTTP 的 HttpRequestDemo、cn.doitedu.Http 因语义服务连接拒绝退出 1；
cn.doitedu.EtlAsync 因 Socket 服务连接拒绝反复重启，观察 90 秒后主动停止。
其余 **23 项流任务均观察到 RUNNING、至少两个成功 checkpoint，ERROR 行数为 0**。
本轮没有 UnknownHostException；RuleEngineRecovery 的两次 checkpoint 已完成。

ETL01 首轮 90 秒未启动完成，补跑完成两个 checkpoint；两个 Local 示例首轮被 checkpoint
观察逻辑过早停止，已修正脚本并补跑至自然退出 0。补跑上限为 180 秒。
ETL04、ETL05、SyncJob01 首轮不足两个 checkpoint，均已补查完成两个。
首次输出保留为 `*.first-attempt.log`，原结果见 [recheck-history.json](recheck-history.json)。

| Java main / 日志 | 退出码 | 主动停止 | RUNNING | 成功 checkpoint | ERROR 行数 | 秒数 |
| --- | ---: | --- | --- | ---: | ---: | ---: |
| [cn.doitedu.rtdw.data_etl.EtlJob01_UserEventsLogCommonDim](cn.doitedu.rtdw.data_etl.EtlJob01_UserEventsLogCommonDim.log) | 143 | True | True | 2 | 0 | 49.94 |
| [cn.doitedu.rtdw.data_etl.EtlJob02_MallTfcAg01](cn.doitedu.rtdw.data_etl.EtlJob02_MallTfcAg01.log) | 143 | True | True | 2 | 0 | 80.13 |
| [cn.doitedu.rtdw.data_etl.EtlJob03_PageAccessTimeLong](cn.doitedu.rtdw.data_etl.EtlJob03_PageAccessTimeLong.log) | 143 | True | True | 2 | 0 | 71.99 |
| [cn.doitedu.rtdw.data_etl.EtlJob04_InteractiveEventsOverview](cn.doitedu.rtdw.data_etl.EtlJob04_InteractiveEventsOverview.log) | 143 | True | True | 2 | 0 | 28.17 |
| [cn.doitedu.rtdw.data_etl.EtlJob05_VideoEventsAgg](cn.doitedu.rtdw.data_etl.EtlJob05_VideoEventsAgg.log) | 143 | True | True | 2 | 0 | 20.6 |
| [cn.doitedu.rtdw.data_etl.EtlJob06_SearchEventsAnalyse](cn.doitedu.rtdw.data_etl.EtlJob06_SearchEventsAnalyse.log) | 143 | True | True | 2 | 0 | 80.81 |
| [cn.doitedu.rtdw.data_etl.EtlJob06_SearchEventsAnalyse_Async](cn.doitedu.rtdw.data_etl.EtlJob06_SearchEventsAnalyse_Async.log) | 143 | True | True | 2 | 0 | 80.93 |
| [cn.doitedu.rtdw.data_etl.EtlJob07_AdRequestSessionProcess](cn.doitedu.rtdw.data_etl.EtlJob07_AdRequestSessionProcess.log) | 143 | True | True | 2 | 0 | 79.8 |
| [cn.doitedu.rtdw.data_sync.SyncJob01_UserInfo2Hbase](cn.doitedu.rtdw.data_sync.SyncJob01_UserInfo2Hbase.log) | 143 | True | True | 2 | 0 | 27.16 |
| [cn.doitedu.rtdw.data_sync.SyncJob02_VideoInfo2Hbase](cn.doitedu.rtdw.data_sync.SyncJob02_VideoInfo2Hbase.log) | 143 | True | True | 2 | 0 | 81.95 |
| [cn.doitedu.rtdw.data_sync.SyncJob03_AdRequestFeatures2Hbase](cn.doitedu.rtdw.data_sync.SyncJob03_AdRequestFeatures2Hbase.log) | 143 | True | True | 2 | 0 | 79.84 |
| [cn.doitedu.rtdw.deployment.StandaloneEventsJob](cn.doitedu.rtdw.deployment.StandaloneEventsJob.log) | 143 | True | True | 2 | 0 | 69.47 |
| [cn.doitedu.rtdw.rt_dashboard.Promotion01_PVUV](cn.doitedu.rtdw.rt_dashboard.Promotion01_PVUV.log) | 143 | True | True | 2 | 0 | 82.25 |
| [cn.doitedu.rtdw.rt_dashboard.Promotion01_PVUVLocal](cn.doitedu.rtdw.rt_dashboard.Promotion01_PVUVLocal.log) | 0 | False | True | 1 | 0 | 49.06 |
| [cn.doitedu.rtdw.utils.DimRecProcess](cn.doitedu.rtdw.utils.DimRecProcess.log) | 0 | False | False | 0 | 0 | 4.63 |
| [cn.doitedu.rtdw.utils.HttpRequestDemo](cn.doitedu.rtdw.utils.HttpRequestDemo.log) | 1 | False | False | 0 | 0 | 11.24 |
| [cn.doitedu.EtlAsync](cn.doitedu.EtlAsync.log) | 143 | True | True | 0 | 0 | 91.08 |
| [cn.doitedu.Http](cn.doitedu.Http.log) | 1 | False | False | 0 | 0 | 11.73 |
| [cn.doitedu.WindowTest](cn.doitedu.WindowTest.log) | 143 | True | True | 2 | 0 | 68.69 |
| [cn.doitedu.rtmk.demo1.TargetProductCartMonitor](cn.doitedu.rtmk.demo1.TargetProductCartMonitor.log) | 143 | True | True | 2 | 0 | 49.68 |
| [cn.doitedu.rtmk.demo1.TargetProductCartMonitorLocal](cn.doitedu.rtmk.demo1.TargetProductCartMonitorLocal.log) | 0 | False | True | 1 | 0 | 48.99 |
| [cn.doitedu.rtmk.demo2.MultiRuleEngine](cn.doitedu.rtmk.demo2.MultiRuleEngine.log) | 143 | True | True | 2 | 0 | 52.78 |
| [cn.doitedu.rtmk.demo2.MultiRuleEngineLocal](cn.doitedu.rtmk.demo2.MultiRuleEngineLocal.log) | 0 | False | True | 1 | 0 | 44.4 |
| [cn.doitedu.rtmk.demo3.RuleCalculatorPattern](cn.doitedu.rtmk.demo3.RuleCalculatorPattern.log) | 143 | True | True | 2 | 0 | 53.8 |
| [cn.doitedu.rtmk.demo3.RuleCalculatorPatternLocal](cn.doitedu.rtmk.demo3.RuleCalculatorPatternLocal.log) | 0 | False | True | 1 | 0 | 8.07 |
| [cn.doitedu.rtmk.demo4.ModelBasedRuleEngine](cn.doitedu.rtmk.demo4.ModelBasedRuleEngine.log) | 143 | True | True | 2 | 0 | 55.92 |
| [cn.doitedu.rtmk.demo4.ModelBasedRuleEngineLocal](cn.doitedu.rtmk.demo4.ModelBasedRuleEngineLocal.log) | 0 | False | True | 1 | 0 | 47.49 |
| [cn.doitedu.rtmk.demo5.BitmapOptimizedRuleEngine](cn.doitedu.rtmk.demo5.BitmapOptimizedRuleEngine.log) | 143 | True | True | 2 | 0 | 39.06 |
| [cn.doitedu.rtmk.demo5.BitmapOptimizedRuleEngineLocal](cn.doitedu.rtmk.demo5.BitmapOptimizedRuleEngineLocal.log) | 0 | False | True | 1 | 0 | 10.08 |
| [cn.doitedu.rtmk.demo6.DynamicRuleManagement](cn.doitedu.rtmk.demo6.DynamicRuleManagement.log) | 143 | True | True | 2 | 0 | 20.73 |
| [cn.doitedu.rtmk.demo6.DynamicRuleManagementLocal](cn.doitedu.rtmk.demo6.DynamicRuleManagementLocal.log) | 0 | False | True | 1 | 0 | 30.39 |
| [cn.doitedu.rtmk.demo7.DynamicRuleManagementV2](cn.doitedu.rtmk.demo7.DynamicRuleManagementV2.log) | 143 | True | True | 2 | 0 | 30.72 |
| [cn.doitedu.rtmk.demo7.DynamicRuleManagementV2Local](cn.doitedu.rtmk.demo7.DynamicRuleManagementV2Local.log) | 0 | False | True | 1 | 0 | 8.6 |
| [cn.doitedu.rtmk.demo8.RuleEngineRecovery](cn.doitedu.rtmk.demo8.RuleEngineRecovery.log) | 143 | True | True | 2 | 0 | 17.57 |
| [cn.doitedu.rtmk.demo8.RuleEngineRecoveryLocal](cn.doitedu.rtmk.demo8.RuleEngineRecoveryLocal.log) | 0 | False | True | 1 | 0 | 7.56 |
| [cn.doitedu.rtmk.demo9.DynamicRuleWithGroovy](cn.doitedu.rtmk.demo9.DynamicRuleWithGroovy.log) | 143 | True | True | 2 | 0 | 18.09 |
| [cn.doitedu.rtmk.demo9.DynamicRuleWithGroovyLocal](cn.doitedu.rtmk.demo9.DynamicRuleWithGroovyLocal.log) | 0 | False | True | 1 | 0 | 6.06 |
| [cn.doitedu.rtmk.demo9.groovy.TestDiaoyong](cn.doitedu.rtmk.demo9.groovy.TestDiaoyong.log) | 0 | False | False | 0 | 0 | 2.02 |
| [cn.doitedu.test.EventsMoni](cn.doitedu.test.EventsMoni.log) | 0 | False | False | 0 | 0 | 2.02 |
| [cn.doitedu.test.HbaseProfileTagsMoni](cn.doitedu.test.HbaseProfileTagsMoni.log) | 0 | False | False | 0 | 0 | 8.07 |
| [cn.doitedu.test.HistoryConditionValueMoni](cn.doitedu.test.HistoryConditionValueMoni.log) | 0 | False | False | 0 | 0 | 1.01 |
| [cn.doitedu.jedis.RedisDemo](cn.doitedu.jedis.RedisDemo.log) | 0 | False | False | 0 | 0 | 1.01 |
