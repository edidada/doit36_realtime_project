# 支付与优惠券风控：Win11 本地全流程

本次在 `fin-risk-engine` 中新增 `cn.doitedu.fin.risk.fraud` 应用；原 Black-Scholes/Monte Carlo 定价代码仍可使用。支付业务风控和金融数值风险是不同领域，分别放在不同包中。

实现提供真实 HTTP、数据库事务、并发约束、持久支付意图、模拟扣款/退款、审核、优惠券和恢复任务。只有交易通道与身份/设备/网络资料是本地合成的；不接入真实资金。Win11 + JDK 17 + Maven 即可运行，运行时无需 Redis、Kafka、Docker、GPU、Python 或付费云服务。

## 1. 一条命令验收

在仓库根目录 PowerShell 中运行：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File fin-risk-engine/scripts/demo-test.ps1
```

脚本先构建，然后生成四份独立随机 token、建立新的 H2 文件数据库、启动隐藏 Java 子进程，依次完成：Alice 领券 → Bob 盗用被拦截 → Alice 下单支付 → 幂等重试 → 查余额 → 全额净支付退款 → 券作废 → 大额支付进入审核 → 独立操作员通过审核 → 扣款超时 → 对账恢复 → 查询审计。最后停止它自己启动的进程。

每次运行使用新的 `.artifacts/fraud/acceptance-<随机ID>/`，保留合成数据库和不包含 token 的日志。不会使用或停止 K3s 作业。若端口被占用，可加 `-Port 18089`。已构建时可加 `-SkipBuild`。

## 2. 启动交互式服务

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File fin-risk-engine/scripts/start-demo.ps1
```

若要在同一 PowerShell 窗口停止后继续使用原 token 和数据库，直接调用脚本：

```powershell
./fin-risk-engine/scripts/start-demo.ps1 -StateDirectory .artifacts/fraud/my-demo
```

地址为 `http://127.0.0.1:8088`，支持 `-Port`。脚本生成的 token 留在当前进程环境中；应用不输出其值。保持该 PowerShell 会话，停止服务后可在同一会话重新启动。若换了窗口，须从自己的凭据管理方式恢复原来的四个环境变量；数据库检测到不同凭据时会拒绝启动，不会自动重置账号和余额。做全新演练时选择新的 `-StateDirectory`。

需要手工调用接口时，可在同一窗口后台启动（环境变量仍留在此窗口）：

```powershell
$riskProcess = ./fin-risk-engine/scripts/start-demo.ps1 -Background -SkipBuild -StateDirectory .artifacts/fraud/interactive-demo
$alice = @{ Authorization = "Bearer $env:RISK_ALICE_TOKEN"; 'Idempotency-Key' = [guid]::NewGuid().ToString() }
$order = Invoke-RestMethod http://127.0.0.1:8088/v1/orders -Method Post -Headers $alice -ContentType application/json -Body '{"productId":"approve","quantity":1}'
$alice['Idempotency-Key'] = [guid]::NewGuid().ToString()
$command = @{ orderId=$order.id; deviceId='alice-phone' } | ConvertTo-Json
$payment = Invoke-RestMethod http://127.0.0.1:8088/v1/payments -Method Post -Headers $alice -ContentType application/json -Body $command
$payment
# 完成后仅停止本次启动的服务。
Stop-Process -Id $riskProcess.Id
```

| 环境变量 | 身份 | 角色/可信演练设备 |
| --- | --- | --- |
| `RISK_ALICE_TOKEN` | alice | CUSTOMER / `alice-phone` |
| `RISK_BOB_TOKEN` | bob | CUSTOMER / `bob-phone` |
| `RISK_NEWBIE_TOKEN` | newbie | CUSTOMER / `newbie-phone`，账号注册 1 小时 |
| `RISK_ADMIN_TOKEN` | operator | ADMIN，只能审核/对账/看审计，不能代用户下单 |

每个 token 至少 32 字符，数据库只存 SHA-256 摘要；应使用启动脚本生成的随机值，不将凭据写入 Git。初始合成钱包每人 10000000 分。服务仅监听 loopback；本地 HTTP 的前提是请求不会离开这台机器。

凭据同时绑定服务端登记的会话设备；请求改填别的 deviceId 不会获得可信设备资格。原会话设备被标记受损时，改填未知设备也不能绕过硬拦截。该绑定仍是沙盒会话资料，不等于真实硬件设备证明。

可直接运行打包后的约 8MB 应用 Jar（四个 token 必须预先设置）：

```powershell
mvn -B -ntp -pl fin-risk-engine -am verify
java -Xmx256m -jar fin-risk-engine/target/fin-risk-engine-1.0-SNAPSHOT-app.jar serve-demo .artifacts/fraud/another-demo
```

旧数值计算入口使用 `java -jar ...-app.jar pricing <原来的定价参数>`，或者继续按 [金融模块说明](finance-modules.md) 调用原 `RiskApplication`。

## 3. 演练商品与优惠活动

金额全部由服务端商品价目生成。请求只能提供商品 ID 和数量，数量 1..100；不能提交实际扣款额、风险分、账号归属、实名标志或支付通道结果。

| 商品 ID | 单价（分） | 合成通道结果 |
| --- | --- | --- |
| `approve` | 10000 | 正常扣款 |
| `decline` | 10000 | 明确拒付 |
| `timeout_after_capture` | 10000 | 首次已扣款但返回 UNKNOWN，重试/对账读取原通道结果 |
| `unavailable` | 10000 | 通道不可用，保留 PENDING，暂不扣款 |
| `small` | 1000 | 正常扣款 |
| `large` | 200000 | 金额异常，通常进入 REVIEW |
| `luxury` | 1100000 | 超单笔硬限额，BLOCKED |

`welcome` 活动用于 `approve/small/large/luxury` 所属商户：满 5000 分减 2000 分；`timeout-promo`、`decline-promo` 分别用于对应模拟商户。每个活动初始 100 张券，启动时起 7 天有效；每人每活动、每设备每活动最多领一张。过期、退款作废均不恢复领取资格，避免重复领取/退款套利。

## 4. HTTP 接口

除 `/health` 外，请求使用 `Authorization: Bearer <token>`。下单、领券、支付必须带 `Idempotency-Key`（1..80 个字母、数字、`-`、`_`）。同一个用户、同一个操作域的 key 只能绑定同一组参数；不同参数返回 409。已完成支付重试返回原支付；PENDING 重试可推进对账，最终结果可能从 PENDING 更新为 PAID。

| 方法和路径 | JSON/说明 |
| --- | --- |
| GET `/health` | 数据库可用才返回 UP |
| POST `/v1/orders` | `{"productId":"approve","quantity":1}` |
| GET `/v1/orders/{id}` | 仅订单本人可读 |
| POST `/v1/coupons/claim` | `{"campaignId":"welcome","deviceId":"alice-phone"}` |
| GET `/v1/coupons/{id}` | 仅领券本人可读 |
| POST `/v1/payments` | `{"orderId":"...","deviceId":"alice-phone","couponId":"..."}`，couponId 可省略 |
| GET `/v1/payments/{id}` | 仅付款人可读 |
| POST `/v1/payments/{id}/review` | ADMIN：`{"approve":true,"reason":"独立审核理由"}` |
| POST `/v1/payments/{id}/cancel` | 本人取消未扣款的 REVIEW，body `{}` |
| POST `/v1/payments/{id}/refund` | 本人申请全额**净支付金额**退款，body `{}`，重复调用不会重复退钱 |
| GET `/v1/payments/{id}/audit` | ADMIN 读取该支付的规则与特征快照、操作人、审核和通道事件 |
| POST `/v1/admin/reconcile` | ADMIN：body `{}`，重试最多 100 条待结算/退款意图 |
| GET `/v1/wallet` | 本人合成 CNY 钱包余额 |

JSON 严格拒绝额外字段、重复字段、浮点数量、null 数量、多个拼接 JSON，body 上限 8KiB；线程池/队列有界，按来源地址 240 次/分钟、用户 120 次/分钟限制请求。IP 取实际连接地址，不信任 `X-Forwarded-For`。应用异常返回 503，不会因异常放行；无 token 返回 401、越权角色 403、他人资源 404。

每次审核都会重查账号、商户、设备、频次和券有效性；硬拦截不能靠操作员批准绕过。普通客户没有审核权限。操作员拒绝审核或客户取消 REVIEW 后释放订单和锁券；PENDING 可能已扣款，因此不能直接取消/释放，需要先对账。尚未提供部分退款；全额退款后券转为 VOID。

## 5. 支付规则与状态

```text
订单 OPEN -> 风控
  BLOCK -> BLOCKED（不调用通道）
  REVIEW -> 人工复核/客户取消（不扣款，券 RESERVED）
  ALLOW/审核通过 -> PENDING（意图持久化，券 RESERVED）
    -> 模拟 PSP 独立事务，按 paymentId 幂等扣款
    -> PAID + 券 USED / DECLINED + 释放订单与券
    -> UNKNOWN 保留 PENDING，后台每 5 秒对账
PAID -> REFUND_PENDING -> PSP 幂等退款 -> REFUNDED + 券 VOID
```

风控输入来自数据库中的账号、设备、网络资料和已发生的交易，不允许 JSON 直接覆盖：

| 因素 | 默认策略 |
| --- | --- |
| 账号封禁、未实名、商户封禁、设备被标记为受损 | 硬拦截 |
| 单笔大于 1000000 分 | 硬拦截 |
| 10 分钟内已有 5 次支付尝试 | 硬拦截，包含被拦截的尝试 |
| 同设备一天内关联至少 3 个其他付款账号 | 硬拦截 |
| 一小时已有 3 次拒付/拦截 | 硬拦截 |
| 未知设备 / 账号新于 24 小时 / 一小时内改密 | +30 / +25 / +35 分 |
| 未知网络 / 代理网络 / 国家不匹配 | +30 / +35 / +30 分 |
| 金额至少 100000 分 / 大于习惯金额 5 倍 | +25 / +20 分 |
| 10 分钟已有至少 3 次尝试 | +25 分 |
| 金额至少 10000 分且达到可用余额 95% | +35 分，防余额搬空 |

硬命中或累计至少 75 分 BLOCK；30..74 分 REVIEW；其余 ALLOW。分数是规则分，不是欺诈概率，也不代表低分交易必然安全。规则版本、完整理由和特征快照随支付持久化。`PaymentView.decision` 是初次风控判定；人工处理与后续状态通过支付状态和审计事件查看。

## 6. 优惠券防盗用与一致性

- 领取：账号需正常且已验证、至少注册 24 小时、近期未改密、设备在本人可信资料中、活动未过期且有库存。同用户/设备的唯一约束和事务锁阻止重复领取和最后一张券超发。
- 核销：检查本人归属、未用/未锁状态、活动时间、商户、币种、最低消费额和优惠额度。Bob 即使知道 Alice 的券 ID 也会被拦截。
- 订单归属、服务端定价和幂等 key 同时校验；两张订单并发使用一张券，只允许一个事务锁券。
- 支付等待/审核时 RESERVED，成功 USED，明确拒付/取消审核回 AVAILABLE，退款 VOID。通道超时不能把 RESERVED 自动释放。
- 模拟 PSP 的独立事务、唯一 charge/refund operation key 和钱包余额约束可复现“通道成功但业务未落库”的故障；对账重试读取原结果。

H2 中一行 `command_lock` 串行化短业务事务，保证本地示例的频次、库存、幂等检查处在同一临界区；它是明确的单机吞吐取舍。实际网络调用在事务之外；模拟 PSP 用独立 JDBC 事务提供持久结果。

## 7. 公开数据下载、回放与 CPU 模型对照

来源是作者 [PaySim](https://github.com/EdgarLopezPhD/PaySim) 公开的合成移动支付数据，正式发布于 [Kaggle ealaxi/paysim1](https://www.kaggle.com/datasets/ealaxi/paysim1)。2026-10-27 实查 Kaggle API：版本 2、CC BY-SA 4.0。原始数据与真实支付记录不同；使用和再分发应保留作者、来源与相同授权。本仓库仅提交程序、报告和自建测试，下载文件放在 Git 忽略的 `.artifacts` 中。

先创建父目录，再执行（下载目录与输出报告应尚不存在；重复运行可使用新的名字）：

```powershell
New-Item -ItemType Directory -Force .artifacts/fraud | Out-Null
$jar = 'fin-risk-engine/target/fin-risk-engine-1.0-SNAPSHOT-app.jar'
java -Xmx256m -jar $jar dataset download .artifacts/fraud/paysim-demo 50000
java -Xmx256m -jar $jar dataset replay .artifacts/fraud/paysim-demo/paysim-prefix.csv .artifacts/fraud/paysim-demo/baseline.json
java -Xmx256m -jar $jar dataset benchmark .artifacts/fraud/paysim-demo/paysim-prefix.csv .artifacts/fraud/paysim-demo/cpu-model.json
java -Xmx256m -jar $jar dataset simulate .artifacts/fraud/paysim-demo/paysim-prefix.csv .artifacts/fraud/paysim-demo/payment-scenarios.json
```

下载器从官方 ZIP 流式读取前 N 条（默认 50000，最多 200000），不会把约 186MB 压缩文件整体落盘，也不会把整个数据集塞入内存。保存 CSV 和 `provenance.json`（来源、下载地址、UTC 时间、样本数、采样方法和 SHA-256），设置网络超时和 32MiB 压缩流预算。失败会报错，**不回退为伪造样本**；如果上游增加登录要求，请从官方页面自行获取 CSV，继续使用 replay/benchmark。

- `replay`：预先固定的简单基线，只使用 type、amount、交易前余额。`newbalance*` 和标签不参与预测。
- `benchmark`：最多 20 万条记录，前 70% 按源顺序训练、后 30% 测试。Java 实现深度 4 的固定网格 CART，最小叶 20，训练类权重封顶 100。保存完整树、训练设置、输入摘要、TP/FP/TN/FN、precision/recall/FPR。不用 GPU，也没有从测试集搜索阈值。测试验证修改 holdout 标签不会改变训练出的树。
- `simulate`：前 20 条公开记录的金额/交易前余额进入本应用的订单、风控和模拟通道。每条是独立场景；CNY、商户、90 天已验证账号、可信设备和 loopback 网络是显式合成前置条件，**不是 PaySim 原始字段**，不计算欺诈准确率。

PaySim 不含设备/IP/实名/优惠券盗用标签，因此离线树不会自动接入在线 API。优惠券与账号攻击由本地合成测试覆盖。详细的实测数值和不足见 [验收报告](payment-coupon-risk-test-report-2026-10-27.md)。

## 8. 对标公开机制及生产差距

| 公开依据 | 本次可运行的对应实现 |
| --- | --- |
| [Stripe Radar 风险评估](https://docs.stripe.com/radar/transaction-risk-prevention)、[规则与频次](https://stripe.com/guides/radar-rules-101) | 多因素规则、频次聚合、拦截理由、人工复核。我们采用 REVIEW 暂不扣款的本地策略，与 Stripe 已扣款后复核的行为有差异。 |
| [Stripe 幂等请求](https://docs.stripe.com/api/idempotent_requests) | key 绑定参数、响应实体唯一、通道操作去重、超时恢复 |
| [微信支付首绑有礼](https://pay.wechatpay.cn/doc/v3/institution/4019727651) | 账号资格、设备防刷、限领、库存/活动期限校验。规则阈值是本项目配置，不宣称复制微信内部模型。 |
| [支付宝开放平台风险评分建议](https://developer.alibaba.com/docs/doc.htm?articleId=105271&docType=1&source=search&treeId=193) | 在领取/下单/支付环节分别控制；中风险留待独立审核 |

本项目落实了可验证的工程机制；“达到大厂生产水平”需要另有证据。目前尚缺真实通道 SDK/签名回调、强身份认证/3DS、真实可信设备证明和 IP 信誉源、密钥轮换、不可篡改审计、真实欺诈回测、漂移监控、多节点高可用与大流量压测。设备 ID 本身不是硬件证明，演练使用预置资料；token 或本机被完全控制后，不能依靠它保证安全。审核依赖独立操作员真实核验，示例中的点击通过不等于完成强身份验证。

当前仅 CNY 合成钱包、单应用进程、固定规则；数据库文件由本机 OS 权限保护，未做静态加密。审计是事务内追加表，数据库管理员仍可改动；幂等和审计保留策略尚未自动归档。后台对账每 5 秒重试，单批 100，尚无真实第三方的指数退避/死信队列。需要上线时应替换这些适配器并验收，而无需为当前本地功能测试设计 GPU 集群。
