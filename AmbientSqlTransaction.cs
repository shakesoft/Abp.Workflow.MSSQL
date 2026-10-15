using System;
using System.Data;
using Microsoft.Data.SqlClient;

// ReSharper disable once CheckNamespace
namespace OptimaJet.Workflow.DbPersistence
{
    /// <summary>
    /// abp-tx: 外部（ABP 工作单元）的环境事务。Accessor 返回 null 时 Provider 行为与官方版一致。
    /// </summary>
    public static class AmbientSqlTransaction
    {
        // ponytail: 进程级静态钩子，一个进程只有一个 WorkflowRuntime；多个 Runtime 需要不同事务源时改成 Provider 实例属性
        public static Func<SqlTransaction> Accessor { get; set; }

        public static SqlTransaction Current
        {
            get
            {
                var tx = Accessor?.Invoke();
                return tx?.Connection == null ? null : tx; // 已提交/回滚的事务 Connection 为 null
            }
        }

        /// <summary>连接属于环境事务时返回该事务，否则 null</summary>
        public static SqlTransaction For(SqlConnection connection)
        {
            var tx = Current;
            return tx != null && ReferenceEquals(tx.Connection, connection) ? tx : null;
        }
    }

    /// <summary>abp-tx: 环境连接只借用不释放</summary>
    public sealed class SqlConnectionLease : IDisposable
    {
        private readonly bool _owned;
        public SqlConnection Connection { get; }

        public SqlConnectionLease(SqlConnection connection, bool owned)
        {
            Connection = connection;
            _owned = owned;
        }

        public void Dispose()
        {
            if (_owned) Connection.Dispose();
        }
    }

    /// <summary>
    /// abp-tx: 替代 connection.BeginTransaction()。
    /// 连接已在环境事务中时改用保存点：Commit 不提交（交给 ABP），Rollback/未 Commit 即释放 只回滚到保存点。
    /// </summary>
    public sealed class LocalSqlTransaction : IDisposable
    {
        private readonly string _savepoint;
        private bool _done;
        public SqlTransaction Transaction { get; }

        private LocalSqlTransaction(SqlTransaction transaction, string savepoint)
        {
            Transaction = transaction;
            _savepoint = savepoint;
        }

        public static LocalSqlTransaction Begin(SqlConnection connection)
        {
            var ambient = AmbientSqlTransaction.For(connection);
            if (ambient == null)
            {
                if (connection.State != ConnectionState.Open) connection.Open();
                return new LocalSqlTransaction(connection.BeginTransaction(), null);
            }

            var savepoint = "wf" + Guid.NewGuid().ToString("N").Substring(0, 30); // SQL Server 保存点名最长 32 字符
            ambient.Save(savepoint);
            return new LocalSqlTransaction(ambient, savepoint);
        }

        public void Commit()
        {
            _done = true;
            if (_savepoint == null) Transaction.Commit();
        }

        public void Rollback()
        {
            _done = true;
            if (_savepoint == null) Transaction.Rollback();
            else Transaction.Rollback(_savepoint);
        }

        public void Dispose()
        {
            if (_savepoint == null)
            {
                Transaction.Dispose(); // 未提交的本地事务由 SqlClient 回滚
                return;
            }

            if (_done || Transaction.Connection == null) return;
            try
            {
                Transaction.Rollback(_savepoint);
            }
            catch (Exception)
            {
                // 事务已不可提交（XACT_STATE = -1）时无法回滚到保存点；吞掉以免掩盖原始异常，
                // 原始异常继续上抛，ABP 工作单元会整体回滚
            }
        }
    }
}
