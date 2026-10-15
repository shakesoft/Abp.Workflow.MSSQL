# Abp.Workflow.MSSQL

WorkflowEngine.NET **14.0.0** 官方 MSSQL Provider 的源码版（来自 `github.com/optimajet/WorkflowEngine.NET` tag `14.0.0`），
改造为可以**加入 ABP 工作单元事务**：工作流的读写和业务代码共用同一个连接、同一个事务，ABP 回滚时流程流转一起回滚。

所有改动处都标注了 `abp-tx:`，升级官方版本时搜索这个标记合并即可。

## 与官方版的区别

| 场景 | 官方 `OptimaJet.Workflow.MSSQL` | 本库 |
|---|---|---|
| 在开启事务的 ABP 工作单元内调用 `WorkflowRuntime` | Provider 自己开连接、自己提交，业务回滚后流程状态已改变 | 借用工作单元的连接和事务，随工作单元一起提交/回滚 |
| Provider 内部的多语句事务 | `connection.BeginTransaction()` | 在环境事务中改用**保存点**（`LocalSqlTransaction`），提交交给 ABP |
| 没有工作单元 / 工作单元未开启事务 | — | 自己新建连接，行为与官方一致 |
| 定时器执行 | `SingleServerTimerManager` | 可选 `AbpUnitOfWorkTimerManager`：每个定时器在独立工作单元中执行，失败回滚并重试 |
| 嵌套 `RequiresNew`/`Suppress` 工作单元操作外层已锁住的流程 | 卡到命令超时 | 立即抛 `NestedWorkflowTransactionException` |

命名空间保持官方的 `OptimaJet.Workflow.DbPersistence`（`MSSQLProvider` 等），业务代码无需改 `using`；
ABP 相关的类型在 `Shakesoft.Workflow.MSSQL` 命名空间。

## 接入步骤

### 1. 引用项目（替换 NuGet 包）

```xml
<!-- 删除官方 MSSQL Provider 的 PackageReference -->
<ProjectReference Include="..\Workflow\Shakesoft.Workflow.MSSQL\Shakesoft.Workflow.MSSQL.csproj" />
```

程序集名是 `Shakesoft.Workflow.MSSQL.dll`，与 NuGet 的 `OptimaJet.Workflow.MSSQL.dll` 不同名。
确保输出目录里**没有**官方的 `OptimaJet.Workflow.MSSQL.dll`（两者类型全名相同会冲突），
`Web.Host/publish.bat` 发布时已经会删除它。

### 2. 依赖模块并指定 DbContext

```csharp
[DependsOn(
    typeof(AbpWorkflowMssqlModule),
    ...)]
public class AutomobileWebCoreModule : AbpModule
{
    public override void PreInitialize()
    {
        // 工作流加入哪个 DbContext 的事务；不设置会在 PostInitialize 抛异常
        Configuration.Get<AbpWorkflowMssqlOptions>().DbContextType = typeof(AutomobileDbContext);
    }
}
```

**要求**：该 DbContext 连接的数据库必须与工作流连接字符串（`appsettings.json` 的 `ConnectionStrings:Workflow`）是**同一个库**，
否则在工作单元内调用工作流会抛 `环境事务数据库 [...] 与工作流数据库 [...] 不一致`。
（跨库无法共享一个 `SqlTransaction`，本库拒绝把工作流数据静默写进业务库。）

### 3. 创建 Runtime

```csharp
var dbProvider = new MSSQLProvider(connectionString); // 与官方用法相同
var builder = new WorkflowBuilder<XElement>(dbProvider, new XmlWorkflowParser(), dbProvider).WithDefaultCache();

var singleServerSettings = new SingleServerSettings
{
    TimerBatchSize = 50,       // 不能为 0（会执行 SELECT TOP 0）
    TimerPeriod = 30 * 1000    // 轮询间隔
};

var runtime = new WorkflowRuntime()
    .WithBuilder(builder)
    .WithPersistenceProvider(dbProvider)
    // ...ActionProvider / RuleProvider / 插件...
    .AsSingleServer(singleServerSettings)
    .WithTimerManager(new AbpUnitOfWorkTimerManager(
        singleServerSettings,
        IocManager.Instance.Resolve<IUnitOfWorkManager>()) { MaxAttempts = 5 })
    .EnableCodeActions();

runtime.Start();
```

Runtime 要在 `PostInitialize` 中创建（依赖其他模块的服务），参见
`src/Shakesoft.Automobile.Application/Workflow/WorkflowManager.cs` 和 `AutomobileWebCoreModule.PostInitialize`。

### 4. 初始化数据库

库内带有 FluentMigrator 迁移（`Migrations/Migration10Initial.cs`，执行嵌入的 `Scripts/InitialWorkflowEngineSchema.sql`），
`MSSQLProvider` 实现了 `IMigratable`（`ConfigureMigrations` 会扫描本程序集），按 WorkflowEngine 官方文档的方式
（`WorkflowEngine.NETCore-Migrator` 包）运行迁移即可。

也可以直接在目标库执行 `Scripts/InitialWorkflowEngineSchema.sql`。

## 使用方式

在应用服务里正常调用 `WorkflowRuntime` 即可，ABP 应用服务方法默认就是带事务的工作单元：

```csharp
public async Task ApproveAsync(Guid processId, string identityId)
{
    var crash = await _crashRepository.GetAsync(id);
    crash.State = "已审核";                                   // 业务写库

    var commands = await WorkflowManager.Runtime.GetAvailableCommandsAsync(processId, identityId);
    await WorkflowManager.Runtime.ExecuteCommandAsync(commands.First(), identityId, null); // 流程流转

    // 之后任何异常 → 业务数据和流程流转一起回滚
}
```

### 注意事项

- **非事务工作单元**（`[UnitOfWork(isTransactional: false)]`）或没有工作单元时，工作流退回独立连接，**不保证原子性**。
- **不要在嵌套的 `RequiresNew` / `Suppress` 工作单元里操作外层已经写过的同一个流程实例**：
  外层持有行锁、又在等内层结束，SQL Server 检测不到这种跨连接等待。本库会直接抛
  `NestedWorkflowTransactionException`，改用默认的 `Required` 加入外层事务即可。
  （检测只按流程实例 Id 判断，扫描到外层锁住的*其他*流程行仍可能阻塞。）
- **流程方案（`WorkflowProcessScheme`）始终用独立连接写入并立即提交**，不随业务回滚。
  它是按方案编码 + 参数编译出的不可变版本，回滚会让其他事务已引用的方案丢失；
  外层回滚最多留下未被引用的方案行，可用 `DropUnusedWorkflowProcessSchemeAsync` 清理。
- 环境事务通过进程级静态钩子 `AmbientSqlTransaction.Accessor` 获取，**一个进程只支持一个 WorkflowRuntime**。
- 数据库访问使用 `Microsoft.Data.SqlClient`；ABP 的事务必须是 `Microsoft.Data.SqlClient.SqlTransaction`，否则抛异常。

## AbpUnitOfWorkTimerManager（定时器）

仅支持**单节点**（`IsSupportsMultiServer = false`），配合 `AsSingleServer(...)` 使用。每个周期：

1. 取出 `TimerBatchSize` 个到期定时器，按根流程分组、串行执行；
2. 先在工作单元**之外**认领定时器（`Ignore = 1` 立即提交）——这样工作单元内第一把锁是流程状态行，
   与用户命令的加锁顺序一致，避免死锁（在工作单元内认领已实测出现 1205 死锁）；
3. 在 `RequiresNew` 工作单元中执行定时器并删除定时器行，Action 中的业务写库与流程流转同属一个事务；
4. 失败 → 整体回滚、取消认领（`Ignore = 0`），下个周期重试；
5. 连续失败 `MaxAttempts`（默认 5）次 → 放弃：定时器保持 `Ignore = 1`，流程置为 `Terminated`
   （与官方 `SingleServerTimerManager` 出错后的结果一致）。

限制：

- 失败计数在内存中，进程重启后重新计数；
- 新登记的定时器最迟一个 `TimerPeriod` 后才执行（`Refresh()` 不立即触发）；
- 串行执行，不像官方实现那样按 CPU 数并行。

不需要定时器事务语义时，仍可用官方的 `.WithTimerManager(new SingleServerTimerManager(settings))`。

## 主要文件

| 文件 | 作用 |
|---|---|
| `AbpWorkflowMssqlModule.cs` | ABP 模块：把当前工作单元的事务接到 `AmbientSqlTransaction`，并提供外层事务链给 `ProcessLockGuard` |
| `AmbientSqlTransaction.cs` | 环境事务钩子、`SqlConnectionLease`（借用的连接不释放）、`LocalSqlTransaction`（保存点） |
| `ProcessLockGuard.cs` | 嵌套事务死等检测 |
| `AbpUnitOfWorkTimerManager.cs` | 工作单元内执行的定时器管理器 |
| `MSSQLProvider.cs` | Provider 主体（官方源码 + `abp-tx` 改动） |
| `FileSchemePersistenceMSSQLProvider.cs` | 方案存文件、其余存 MSSQL 的变体（官方） |
| `Models/` | 各表的数据访问对象（官方源码） |
| `Migrations/`、`Scripts/` | 建表迁移与 SQL 脚本 |

## 升级 WorkflowEngine 版本

1. 修改 `Directory.Build.props` 中的 `$(WorkflowEngineVersion)`；
2. 拉取对应 tag 的官方 MSSQL Provider 源码，对比本目录，重新应用所有 `abp-tx:` 标记处的改动；
3. 若官方 `TimerManagerBase` / `ITimerManager` 签名变化，同步修改 `AbpUnitOfWorkTimerManager`。
