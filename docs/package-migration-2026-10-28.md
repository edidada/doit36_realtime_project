# 包名迁移说明（2026-10-28）

所有业务模块统一使用 `com.test.bigdata`，Maven 聚合工程及子模块的 `groupId` / parent 坐标同步更新。artifactId、版本号、JAR 文件名和业务逻辑不变。源码目录同步迁移为 `src/main/java/com/test/bigdata` 和 `src/test/java/com/test/bigdata`，包括 Scala 与动态 Groovy 示例。

## 验证方法

在仓库根目录，使用 JDK 17：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/check-package-layout.ps1
mvn -B -ntp clean verify
powershell -NoProfile -ExecutionPolicy Bypass -File fin-risk-engine/scripts/demo-test.ps1 -SkipBuild
```

CI 已增加包名/路径检查并使用全量清理构建。`PackageMigrationTest` 直接编译仓库内 Groovy 规则并验证其实现迁移后的 Java 接口，另验证动态脚本实现迁移后的 `HelloInterface`。支付验收通过可执行 JAR 的新入口运行。

`master` 本地结果：JDK 17 下 `clean verify` 成功，69 项测试通过（Groovy 迁移 2、OLAP 21、risk 46；旧 `WindowTest` 无测试方法，不计入）；125 个源码文件包声明与路径一致；6 个模块 JAR 均含新包路径且不含旧业务包路径；支付/优惠券 14 项真实 HTTP 验收通过。数值定价入口以新全限定类名实际运行成功。

检查器覆盖 Git 管理的源码、POM、脚本、部署文件和文档，排除 `logs/` 历史原始日志；原始日志不改写，以免将旧执行结果伪造成新包名的运行证据。历史报告的类名/源码链接更新仅为便于阅读，不等于重跑原测试。

## 使用与兼容性

- IDEA 重新加载 Maven；项目仍固定 JDK 17。不要继续使用缓存的旧包名运行配置。
- 从 `clean` 构建生成全部 JAR；第三方依赖本身的包名不变。
- Flink 的 `entryClass`、`flink run -c` 和本地 `java` 主类参数使用新包名，必须配合新 JAR/镜像，不能只改部署文件继续使用旧镜像。
- 课程环境的 `doitedu` 主机名及数据表、Topic、服务地址不属于 Java 包名，本次不修改。
- 本次没有部署集群或迁移在线状态。Flink checkpoint/savepoint、Java 序列化对象和存量 Groovy/数据库规则源码可能含旧类名；不能直接承诺旧状态兼容。上线前备份状态与规则，独立验证恢复，必要时设计显式状态迁移或从可回放源重建，禁止直接删除生产状态。
- Maven 消费方需改用 `com.test.bigdata` groupId；旧坐标不会自动重定向。

## k3s 合并后的补齐与验证

先提交 `master` 的迁移，再将 `master` 合并到 `k3s`。导入冲突按保留 K3s `RuntimeConfig` / 故障恢复逻辑并迁移包名的方式解决，没有用 master 的教学环境实现覆盖 K3s 实现。与合并前 `k3s` 逐一比对，128 个原有源码文件除包名和路径替换外完全一致。

补齐了该分支独有的 `RuntimeConfig`、`StandaloneEventsJob`、ETL 配置测试、Groovy 示例、部署脚本及 YAML 的 `entryClass`、镜像中 JAR 打包目录、main 执行目录和规则初始化/恢复验证脚本。

本地验收（JDK 17 / Windows）：

- `mvn -B -ntp clean verify`：7 个 reactor 工程成功；70 项测试通过（ETL 配置 1、Groovy 迁移 2、OLAP 21、risk 46），无失败/跳过。
- 包名检查：129 个源码文件路径一致；活动代码、部署文件、脚本与文档不含旧包名引用；`logs/` 原始历史快照保留不变。
- 6 个业务模块 JAR：均含新包路径且无旧业务包路径。
- 支付/优惠券真实 HTTP 验收：14 项全部通过。
- 修改的 Bash 脚本语法通过；`java-mains.sh --list` 的 42 个入口与迁移后的源码路径、包声明全部对应。

以上不包含真实 K3s 集群部署、外部中间件全链路或旧 savepoint 恢复，不以历史成功的 GitHub Actions 代替本次提交的 CI 结果。
