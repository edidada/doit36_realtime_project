# FIN-OLAP / FIN-RISK Maven 模块交付说明

2026-10-27 更新：`fin-risk-engine` 已增加支付/优惠券业务风控、嵌入式持久化、模拟通道和公开数据回放，详见 [独立功能手册](payment-coupon-risk-guide.md)。以下数值定价部分保留原有实现与当时验收范围。

## 实施范围

用户要求在当前仓库增加两个 Maven module，因此将 [设计基线](design/maven_module.md) 中建议拆为两个仓库的系统，先以 `fin-olap-engine`、`fin-risk-engine` 两个独立 Jar 模块交付。它们不依赖已有 Flink/Spark 模块，继承根 POM 的 JDK 17；本次明确指定 Maven，未另建 Gradle 分支。

本次落实设计文档 V0.1 的单机计算阶段，并提前实现分组聚合、有限并行模拟、历史 VaR/ES。设计中的十亿行、多节点、99.9% 可用性仍是未验收目标。

| 设计要求 | 本次实现 | 验证/剩余工作 |
| --- | --- | --- |
| CSV / Parquet 扫描 | `CsvTransactionSource` / `ParquetTransactionSource` | 相同交易数据跨格式逐组对账；NIO 本地读取，无需 winutils.exe |
| 日期过滤、SUM、COUNT、GROUP BY | `ReportEngine` | 包含首尾日期、退款负金额、跨币种隔离和独立 BigInteger 随机对账 |
| 金额不得静默溢出 | `Math.addExact` | 正负溢出都使整次查询失败 |
| 查询资源限制 | 最大扫描行数、分组数、超时、线程中断 | 无部分成功；分组数限制并非精确 JVM 内存额度，无 Spill |
| 快照可追溯 | snapshotId + 输入 SHA-256、执行前后内容校验 | 调用者须提供不可变文件；校验不替代存储系统的版本化快照 |
| 报表导出 | 严格 UTF-8 CSV、固定排序、临时文件完成后发布 | 不覆盖现有目标文件；尚无 Parquet 结果导出 |
| Black-Scholes 和 Greeks | 欧式看涨/看跌，连续股息，Delta/Gamma/Vega | 已验证参考值、put-call parity 和有限差分 |
| Monte Carlo | GBM 终值、路径编号随机流、固定顺序归约 | 固定 seed/模型/输入时，1/2/4 线程逐位一致；返回标准误和近似 95% 区间 |
| VaR / ES | 已提供历史损失样本库接口 | nearest-rank VaR；ES 为所有大于等于 VaR 的观测值均值，含边界重复值；未实现持有期转换与回测 |
| Arrow / Calcite / SQL / SIMD / JMH | 未实现 | 属于 V0.2/V0.3 后续工作 |
| 组合、债券、远期、曲线、二叉树 | 未实现 | 当前只估值单份欧式期权；尚无外汇/货币换算 |
| Coordinator / Worker / Shuffle / 恢复 | 未实现 | 本次仅单机；没有持久任务状态、远程 RPC 或集群部署 |
| REST/gRPC、认证、审计持久化、监控 | 未实现 | 当前是普通 Java 库与命令行入口 |

## Win11 编译与测试

在仓库根目录使用 PowerShell，确认 `mvn -v` 显示 Java 17：

```powershell
mvn -v
mvn -B -ntp -pl fin-olap-engine,fin-risk-engine -am verify
```

运行整个仓库：

```powershell
mvn -B -ntp verify
```

新增测试为 FIN-OLAP 21 个、FIN-RISK 17 个，均含断言；没有 Kafka/MySQL/HBase/Doris 前置依赖。Parquet 测试自行在临时目录创建文件，并验证读取后的结果和 CSV 一致。测试运行中的临时文件和 `target` 不应提交。

CI 在 `master` 和 `k3s` 推送时运行 Linux 全 Reactor `verify`，并运行 Windows 上两个金融模块的 `verify`。CI 成功代表编译和功能回归通过，不构成十亿行或生产金融模型验收。

## FIN-OLAP 快速运行

从仓库根目录执行（输出文件必须尚不存在）：

```powershell
mvn -B -ntp -f fin-olap-engine/pom.xml compile exec:java '-Dexec.args=examples/finance/transactions.csv fin-olap-engine/target/example-report.csv snapshot-202601 2026-01-01 2026-01-31'
Get-Content fin-olap-engine/target/example-report.csv
```

预期输入 5 行、日期范围内 4 行，输出 3 组：

```csv
trade_date,account_id,institution_id,transaction_type,currency,trade_count,total_amount_cents
2026-01-01,account-a,bank-a,PAY,CNY,2,12000
2026-01-01,account-a,bank-a,PAY,USD,1,2000
2026-01-31,account-b,bank-a,TRANSFER,CNY,1,30000
```

控制台输出 snapshotId、输入 SHA-256、扫描/匹配行数、分组数。库返回的 `ReportResult` 是不可变结果。

命令参数：

```text
input.csv|input.parquet output.csv snapshotId startDate endDate
  [maxGroups=100000] [maxScannedRows=10000000] [timeoutSeconds=60]
```

输入 CSV 的标题及顺序必须为：

```text
trade_id,trade_date,account_id,institution_id,transaction_type,currency,amount_cents
```

Parquet 使用同名 Avro 字段：前六列是非空字符串，日期是 ISO `yyyy-MM-dd` 字符串，`amount_cents` 为 `long`，不是浮点数。暂不接受 logical date 或 decimal 类型。CSV 支持 RFC 4180 引号、逗号和换行。

金额按币种最小单位存储；示例 CNY 12500 表示 125.00 元。系统不做汇率换算，币种总是分组键。空值、缺列、无效日期、非法币种码、非整数金额、超范围金额均使查询失败。每行按一次交易计数，不按 trade_id 去重；输入快照负责避免重复交易。

文件会流式读取，分组在内存中保存，默认最多 100000 组。执行前后还会各扫描一次文件计算 SHA-256；其时间计入超时。超时在读取批次/记录边界检查，无法中断一个阻塞的底层 I/O 调用。该实现没有 Arrow 批次和谓词下推；不要把默认行数上限当成性能承诺。

输出 CSV 用于程序间交换。若外部输入的账号等字段可能以 Excel 公式字符开头，在表格软件中应按文本导入。

## FIN-RISK 快速运行

```powershell
mvn -B -ntp -f fin-risk-engine/pom.xml compile exec:java '-Dexec.args=md-20261026 2026-10-26 CALL 100 100 0.05 0 0.2 1 250000 42 4'
```

参数顺序：

```text
snapshotId valuationDate CALL|PUT spot strike rate dividendYield volatility maturityYears paths
  [seed=42] [workers=1] [instrumentId=option]
```

该示例解析价约为 `10.4505835722`；25 万路径、seed=42 的模拟价约为 `10.4731203626`，标准误约为 `0.0295102682`。模拟结果与解析解的差异应按标准误评估，不应要求两个数完全相等。

利率和波动率按小数输入：`0.05` 为 5% 年化连续复利利率，`0.2` 为 20% 年化波动率。期限是调用方提供的年化数，不自动处理交易日历/日计数。请求包含估值日期和市场快照标识，结果附带快照 ID 与模型版本；估值日期、合约和完整请求参数尚未复制进结果对象，调用方须一同留存，当前没有持久审计存储。模型仅接受正的 spot/strike/volatility/maturity，负利率可用。到期、零波动及 NaN/Infinity 参数会明确拒绝。Vega 按波动率变化 1.0 计算，如需每 1% 波动率敏感度须除以 100。

模拟路径数限为 2..10000000，线程数限为 1..32。统计使用 Welford/Chan 稳定方差和固定 4096 路径分块，随机数来自按 seed/路径编号派生的 SplitMix64 + Box-Muller。模型版本为 `gbm-box-muller-splitmix64-1`，不可在保持该版本号时更改随机流。95% 区间是正态近似，小样本或极端参数需额外验证。中断或数值异常使整个任务失败，不返回部分成功。

历史 VaR/ES 使用 `HistoricalRisk.calculate(double[] losses, double confidence)`；正数代表损失，负数代表盈利，输入数组不被排序修改。接口没有默认持有期、估值口径和市场数据，因此调用者须将这类元数据与样本一同管理。此处 ES 使用明确的样本条件均值口径，尚非生产监管风险引擎。

## 本地验收记录与分支顺序

- Win11、JDK 17.0.12、Maven 3.9.11：两个金融模块 38 个断言测试通过，全 Reactor 7 个工程构建通过。
- 本地发现并修复 Parquet 损坏文件异常归类；排除了新模块中 Hadoop 传递引入的旧 Log4j/reload4j 实现，采用 JDK 日志适配。
- 开发提交先位于 `master`；随后在 `k3s` 分支合并 `master`，不把 K3s 专用镜像/部署差异反向带入主开发分支。
- 通过 `gh` 读取了旧 K3s CI `40187831471` 的失败日志：ETL 合并引入重复变量、缺失 `sqlValue`/`environmentOrDefault` 方法。该分支修复和新提交对应的远端 CI 证据以实际运行记录为准。
