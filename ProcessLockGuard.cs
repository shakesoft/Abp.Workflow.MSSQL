using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Microsoft.Data.SqlClient;

// ReSharper disable once CheckNamespace
namespace OptimaJet.Workflow.DbPersistence
{
    /// <summary>
    /// abp-tx: 嵌套事务（RequiresNew/Suppress）操作被外层事务锁住的流程实例时立即报错。
    /// 外层事务要等内层执行完才能提交，内层又在等外层的行锁，SQL Server 检测不到这种跨连接的等待，只会一直卡到命令超时。
    /// </summary>
    public static class ProcessLockGuard
    {
        /// <summary>当前调用链上的外层事务（不含当前事务），由宿主提供；为 null 时不做检测</summary>
        public static Func<IEnumerable<SqlTransaction>> OuterTransactionsAccessor { get; set; }

        // 事务 -> 该事务写过（持有行锁）的流程实例；事务对象被回收后自动清除
        private static readonly ConditionalWeakTable<SqlTransaction, ConcurrentDictionary<Guid, byte>> Locked = new();

        /// <summary>读流程数据前调用（ReadCommitted 下读也会被外层的排他锁阻塞）</summary>
        public static void Read(Guid processId) => ThrowIfLockedByOuter(processId);

        /// <summary>写流程数据前调用：检测后记录当前事务锁住了该流程</summary>
        public static void Write(Guid processId)
        {
            ThrowIfLockedByOuter(processId);
            var tx = AmbientSqlTransaction.Current;
            if (tx != null)
            {
                Locked.GetOrCreateValue(tx).TryAdd(processId, 0);
            }
        }

        // ponytail: 只按流程实例 Id 判断；按 ProcessId 无索引的扫描若碰到外层锁住的其他流程行仍可能阻塞，出现时再按表扩大判断范围
        private static void ThrowIfLockedByOuter(Guid processId)
        {
            var outers = OuterTransactionsAccessor?.Invoke();
            if (outers == null) return;

            foreach (var outer in outers)
            {
                if (outer?.Connection != null // 已提交/回滚的事务不再持锁
                    && Locked.TryGetValue(outer, out var processes)
                    && processes.ContainsKey(processId))
                {
                    throw new NestedWorkflowTransactionException(processId);
                }
            }
        }
    }

    public class NestedWorkflowTransactionException : InvalidOperationException
    {
        public Guid ProcessId { get; }

        public NestedWorkflowTransactionException(Guid processId)
            : base($"流程实例 {processId} 已被外层事务锁定，嵌套的 RequiresNew/Suppress 工作单元不能再操作它（会一直阻塞到超时）。" +
                   "请改用默认的 Required 工作单元加入外层事务。")
        {
            ProcessId = processId;
        }
    }
}
