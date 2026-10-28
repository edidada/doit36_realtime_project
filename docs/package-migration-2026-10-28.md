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
