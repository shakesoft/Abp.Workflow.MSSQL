using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Abp.Data;
using Abp.Domain.Uow;
using Abp.Modules;
using Microsoft.Data.SqlClient;
using OptimaJet.Workflow.DbPersistence;

namespace Shakesoft.Workflow.MSSQL
{
    /// <summary>
    /// 让 MSSQLProvider 加入当前 ABP 工作单元的事务：同一连接、同一事务，ABP 回滚则工作流一起回滚。
    /// 没有工作单元或工作单元未开启事务时（定时器、后台线程），Provider 自己新建连接，与官方行为一致。
    /// 需在依赖模块的 PreInitialize 中设置：Configuration.Get&lt;AbpWorkflowMssqlOptions&gt;().DbContextType = typeof(XxxDbContext)
    /// </summary>
    public class AbpWorkflowMssqlModule : AbpModule
    {
        // 工作单元 -> 它的工作流事务；只有用过工作流的工作单元才会登记（也只有它们可能持有工作流行锁）
        private static readonly ConditionalWeakTable<IUnitOfWork, SqlTransaction> UowTransactions = new();

        public override void PreInitialize()
        {
            IocManager.Register<AbpWorkflowMssqlOptions>();
        }

        public override void PostInitialize()
        {
            var options = IocManager.Resolve<AbpWorkflowMssqlOptions>();
            if (options.DbContextType == null)
            {
                throw new Abp.AbpException($"{nameof(AbpWorkflowMssqlOptions)}.{nameof(AbpWorkflowMssqlOptions.DbContextType)} 未设置");
            }

            var uowProvider = IocManager.Resolve<ICurrentUnitOfWorkProvider>();
            var txProvider = IocManager.Resolve<IActiveTransactionProvider>();

            AmbientSqlTransaction.Accessor = () =>
            {
                var uow = uowProvider.Current;
                if (uow == null)
                {
                    return null;
                }

                // 会按需创建 DbContext 并开启 ABP 事务；非事务工作单元返回 null（工作流退回独立连接，不保证原子性）
                var tx = txProvider.GetActiveTransaction(new ActiveTransactionProviderArgs
                {
                    { "ContextType", options.DbContextType },
                    { "MultiTenancySide", null }
                });
                if (tx == null)
                {
                    return null;
                }

                var sqlTx = tx as SqlTransaction ?? throw new InvalidOperationException(
                    $"ABP 事务类型为 {tx.GetType().FullName}，需要 Microsoft.Data.SqlClient.SqlTransaction");
                UowTransactions.AddOrUpdate(uow, sqlTx);
                return sqlTx;
            };

            // 当前工作单元之外的外层工作单元（RequiresNew/Suppress 才会形成 Outer 链，Required 直接复用外层）
            ProcessLockGuard.OuterTransactionsAccessor = () => OuterTransactions(uowProvider.Current);
        }

        private static IEnumerable<SqlTransaction> OuterTransactions(IUnitOfWork current)
        {
            for (var outer = current?.Outer; outer != null; outer = outer.Outer)
            {
                if (UowTransactions.TryGetValue(outer, out var tx))
                {
                    yield return tx;
                }
            }
        }

        public override void Shutdown()
        {
            AmbientSqlTransaction.Accessor = null;
            ProcessLockGuard.OuterTransactionsAccessor = null;
        }
    }

    public class AbpWorkflowMssqlOptions
    {
        public Type DbContextType { get; set; }
    }
}
