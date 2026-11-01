# Log4j2 与 RiskApplication 运行说明（2026-10-29）

## 直接复制的两行 Maven 命令

在仓库根目录，用 JDK 17 执行：

```powershell
mvn -B -ntp -pl fin-risk-engine -am verify
mvn -q -f fin-risk-engine/pom.xml exec:java
```

第一行编译、测试并打包；第二行运行 `com.test.bigdata.fin.risk.RiskApplication`。两行都已实际运行成功。首次 Maven 需联网下载依赖；测试数据已经打进 JAR，运行时不需要联网、Redis、Kafka、GPU 或 Docker。

原报错原因是 main 要求至少 10 个参数，而 IDEA 未传入任何参数。现在零参数会明确运行公开基准模式，不再直接抛 Usage 异常；传入一部分参数仍会报错，防止悄悄用演示值替代用户输入。

无参数默认执行 QuantLib 官方开源数据中的 8 个欧式期权样例，逐例输出上游期望值、解析定价、绝对误差、容差、Monte Carlo 定价/标准误差/95% 置信区间。末行：

```text
Public benchmark complete: 8/8 analytic prices passed; pathsPerCase=100000 seed=42 workers=1
```

首例实际结果：期望 `2.1334`，解析值 `2.1333684449162007`，误差 `0.00003155508379926175 < 0.0001`；Monte Carlo `2.1275466069626736`，95% 区间 `[2.0996140640004444, 2.155479149924903]`。Monte Carlo 区间不是业务支付风险分数，也不是交易建议。

## 公开测试数据来源

数据来自 [QuantLib 1.40 官方 EuropeanOptionTests/testValues](https://github.com/lballabio/QuantLib/blob/06edddbdb85f006c43a449b5fc846a00c67477f3/test-suite/europeanoption.cpp#L261-L315)，固定提交 `06edddbdb85f006c43a449b5fc846a00c67477f3`，取原数组前 8 项；不是自行捏造的市场行情。字段、来源哈希和版权说明在 [数据说明](../fin-risk-engine/src/main/resources/datasets/README.md)，[CSV](../fin-risk-engine/src/main/resources/datasets/quantlib-european-options.csv) 与完整上游许可证随 JAR 分发。

它们是公开数值回归案例，不是真实报价数据；本地固定估值日期只是程序元数据。支付反欺诈仍使用现有 PaySim 公共合成数据工具，参见 [支付与优惠券指南](payment-coupon-risk-guide.md)，不要把期权定价程序与支付是否放行混为一谈。

## 显式参数与 IDEA

仍可自行传原有的 10～13 个参数；以下是上游首例，不是随意填数：

```powershell
mvn -q -f fin-risk-engine/pom.xml exec:java "-Dexec.args=quantlib-value-01 2026-01-01 CALL 60 65 0.08 0 0.30 0.25 100000 42 1 quantlib-call-01"
```

参数顺序是：`snapshotId valuationDate CALL|PUT spot strike rate dividendYield volatility maturityYears paths [seed] [workers] [instrumentId]`。利率/分红率/波动率使用小数，例如 `0.08=8%`。日期不改变上游年化期限。

IDEA 重新加载 Maven，Project SDK 与运行配置 JRE 选 JDK 17；运行 `RiskApplication` 时 Program arguments 可留空跑公开样例，或填写上面的参数串（不含 Maven 的 `-Dexec.args=` 包装）。Working directory 建议仓库根目录；不要继续使用只有旧依赖的缓存 classpath。

可执行 JAR 也已用用户的 `D:\develops\jdks\ms-17.0.17\bin\java.exe` 实际验证：

```powershell
java -jar fin-risk-engine/target/fin-risk-engine-1.0-SNAPSHOT-app.jar pricing
```

该 JAR 默认入口是支付沙箱 `FraudApplication`，因此数值定价必须带 `pricing` 子命令。

## 日志门面、实现与 XML

应用统一通过 SLF4J 门面输出；实现为 Log4j2 `2.26.1`。父 POM 统一 API/Core 版本，每个模块 `src/main/resources/log4j2.xml` 配置控制台和独立 RollingFile。SLF4J 1.7 使用 `log4j-slf4j-impl`，Spark 的 SLF4J 2 使用 `log4j-slf4j2-impl`，依据 [Log4j 官方安装说明](https://logging.apache.org/log4j/2.x/manual/installation.html)。运行时探针检查实际 provider、Core 版本/来源、绑定数量、XML appender 和落盘结果，不以依赖树替代真实运行验证。

替换了 OLAP、数值定价、PaySim 下载/评估、支付服务启动/异常/恢复、Redis 示例及 HTTP 示例里的直接打印。请求失败日志仍只记异常类别，不记录 Bearer token、请求体或底层异常消息。Flink `.print()` 是业务结果 sink，不是应用诊断日志，本次不改其数据流语义，也不宣称它会写入 Log4j2 文件。

旧的两份 `log4j.properties` 已移除，改用 XML；旧 Log4j 1.x/reload4j 实现依赖被排除，遗留 API 调用使用 Log4j2 兼容桥接，避免与新实现并存。

默认写到 JVM 工作目录下 `logs/runtime/<module>.log`；控制台为 stderr，文件 UTF-8，追加写，按日/10MB 滚动，并在滚动时清理超过 7 天的归档。可覆盖目录：

```powershell
mvn -q -f fin-risk-engine/pom.xml exec:java "-Drealtime.log.dir=logs/verification/my-run"
```

未筛选的运行日志默认忽略，防止误交令牌/个人数据。本次经检查的 6 个模块日志特意保存在 [logs/verification/2026-10-29](../logs/verification/2026-10-29/README.md)，可正常 `git add`。示例日志只包含探针元数据、公开期权案例、OLAP 的合成数据统计，不包含账户令牌、数据库或私有支付数据。

## 本地验收结果

- `mvn -B -ntp clean verify`：7 个 reactor 工程成功，81 项测试通过，无失败/跳过（risk 56、OLAP 23、Groovy 迁移 2；旧 WindowTest 0 项不计）。
- 两行 Maven 命令、显式参数模式、JDK 17 可执行 JAR 模式：全部退出码 0。
- 六模块日志探针：全部验证 `SLF4J -> Log4j2 2.26.1 -> XML -> 文件`，无重复绑定或旧实现；探针不访问外部中间件，不等于 Redis/Kafka 业务验收。
- 支付/优惠券真实 HTTP 验收：14 项通过，改日志后仍可正常扣款、退款、拦截盗券和超时恢复。
- OLAP 实际 CSV 运行：scanned=5、matched=4、groups=3，报表和文件日志正常。此 CSV 仍是明确标注的本地合成示例，不冒充公开金融行情。
- GitHub Actions 已加入日志探针；查询到的历史 CI 不代表这些尚未推送修改已在远端通过。

复验日志：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-logging.ps1 -SkipBuild -LogDirectory logs/verification/2026-10-29
```
