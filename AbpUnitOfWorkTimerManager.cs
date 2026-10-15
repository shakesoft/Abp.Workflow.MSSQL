using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Abp.Domain.Uow;
using Abp.Logging;
using OptimaJet.Workflow.Core.Persistence;
using OptimaJet.Workflow.Core.Runtime;
using OptimaJet.Workflow.Core.Runtime.Settings;
using OptimaJet.Workflow.Core.Runtime.Timers;
using OptimaJet.Workflow.DbPersistence;
using ModelTimer = OptimaJet.Workflow.Core.Model.WorkflowTimer;

namespace Shakesoft.Workflow.MSSQL
{
    /// <summary>
    /// 单节点定时器管理器：每个到期定时器在独立的 ABP 工作单元中执行。
    /// 流程流转与 Action 里的业务写库同属一个事务，Action 抛异常则全部回滚，取消认领后下个周期重试；
    /// 连续失败 MaxAttempts 次后放弃：定时器置 Ignore、流程置 Terminated（与官方 SingleServerTimerManager 出错后的结果一致）。
    /// 用法：runtime.AsSingleServer(settings).WithTimerManager(new AbpUnitOfWorkTimerManager(settings, unitOfWorkManager))
    /// </summary>
    public class AbpUnitOfWorkTimerManager : TimerManagerBase, ITimerManager
    {
        private readonly SingleServerSettings _settings;
        private readonly IUnitOfWorkManager _unitOfWorkManager;

        // ponytail: 失败计数在内存里，进程重启后重新计数；需要跨重启累计时再落库
        private readonly ConcurrentDictionary<Guid, int> _failures = new();

        private CancellationTokenSource _cts;
        private Task _loop;

        /// <summary>同一定时器连续失败多少次后放弃（每次间隔 TimerPeriod）</summary>
        public int MaxAttempts { get; set; } = 5;

        public AbpUnitOfWorkTimerManager(SingleServerSettings settings, IUnitOfWorkManager unitOfWorkManager)
        {
            _settings = settings;
            _unitOfWorkManager = unitOfWorkManager;
        }

        public void Init(WorkflowRuntime runtime)
        {
            _runtime = runtime;
        }

        public bool IsSupportsMultiServer => false;

        protected override bool Stopped => _loop == null || _loop.IsCompleted;

        public Task StartAsync(int? timeout) => StartTimerInternalAsync();

        public void Stop(int? timeout)
        {
            StopTimerInternal();
            try
            {
                _loop?.Wait(timeout ?? Timeout.Infinite);
            }
            catch (AggregateException)
            {
                // 循环被取消
            }
        }

        // 轮询间隔固定为 TimerPeriod，新登记的定时器最迟一个周期后执行
        public override void Refresh()
        {
        }

        protected override Task StartTimerInternalAsync()
        {
            if (_loop != null && !_loop.IsCompleted) return Task.CompletedTask;
            _cts = new CancellationTokenSource();
            var token = _cts.Token;
            _loop = Task.Run(() => LoopAsync(token));
            return Task.CompletedTask;
        }

        protected override void StopTimerInternal()
        {
            _cts?.Cancel();
        }

        private async Task LoopAsync(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                try
                {
                    await ProcessDueTimersAsync(token).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    LogHelper.Logger.Error("工作流定时器轮询失败", ex);
                }

                try
                {
                    await Task.Delay(_settings.TimerPeriod, token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }

        // ponytail: 按根流程分组后串行执行；官方实现按 ProcessorCount × MaxDegreeOfParallelismMultiplier 并行，批量大了再并行
        private async Task ProcessDueTimersAsync(CancellationToken token)
        {
            var timers = await _runtime.PersistenceProvider.GetTopTimersToExecuteAsync(_settings.TimerBatchSize).ConfigureAwait(false);

            // 失败过的定时器若已被删除（流程被命令推进、被删除），计数不会再被清理；只保留本批仍到期的，防止字典无限增长
            var dueIds = timers.Select(t => t.TimerId).ToHashSet();
            foreach (var timerId in _failures.Keys.Where(id => !dueIds.Contains(id)).ToList())
            {
                _failures.TryRemove(timerId, out _);
            }

            foreach (var timer in timers.GroupBy(t => t.RootProcessId).SelectMany(g => g.OrderBy(t => t.NextExecutionDateTime)))
            {
                if (token.IsCancellationRequested) return;
                await ExecuteInUnitOfWorkAsync(timer).ConfigureAwait(false);
            }
        }

        private async Task ExecuteInUnitOfWorkAsync(ModelTimer timer)
        {
            var provider = (MSSQLProvider)_runtime.PersistenceProvider;

            // 认领在工作单元之外立即提交（与官方一致）：工作单元内第一把锁就是流程状态行，与用户命令的加锁顺序
            // （状态行 -> 定时器行）相同。若在工作单元内认领，会先锁定时器行，与同时执行的用户命令形成死锁（已实测 1205）
            if (await provider.SetTimerIgnoreAsync(timer.TimerId).ConfigureAwait(false) == 0)
            {
                return; // 已被其他执行认领或已删除
            }

            try
            {
                using (var uow = _unitOfWorkManager.Begin(TransactionScopeOption.RequiresNew))
                {
                    await _runtime.ExecuteTimerAsync(timer.ProcessId, timer.Name).ConfigureAwait(false);
                    await provider.DeleteTimerAsync(timer.TimerId).ConfigureAwait(false);
                    await uow.CompleteAsync().ConfigureAwait(false);
                }

                _failures.TryRemove(timer.TimerId, out _);
            }
            catch (Exception ex)
            {
                var attempts = _failures.AddOrUpdate(timer.TimerId, 1, (_, n) => n + 1);
                LogHelper.Logger.Error($"工作流定时器执行失败，已回滚（第 {attempts}/{MaxAttempts} 次）：流程 {timer.ProcessId}，定时器 {timer.Name}", ex);
                if (attempts >= MaxAttempts)
                {
                    _failures.TryRemove(timer.TimerId, out _);
                    await GiveUpAsync(timer).ConfigureAwait(false);
                }
                else
                {
                    await RearmAsync(provider, timer).ConfigureAwait(false);
                }
            }
        }

        // 工作单元已回滚：取消认领，下个周期重试；若定时器已被删除（流程已离开该活动）则更新 0 行，无副作用
        private static async Task RearmAsync(MSSQLProvider provider, ModelTimer timer)
        {
            try
            {
                await provider.ClearTimerIgnoreAsync(timer.TimerId).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                LogHelper.Logger.Error($"工作流定时器重新武装失败，将不再触发：流程 {timer.ProcessId}，定时器 {timer.Name}", ex);
            }
        }

        // 工作单元已结束，以下写入走 Provider 自己的连接直接提交；定时器保持已认领（Ignore=1）不再触发
        private async Task GiveUpAsync(ModelTimer timer)
        {
            try
            {
                await _runtime.PersistenceProvider.SetProcessStatusAsync(timer.ProcessId, ProcessStatus.Terminated).ConfigureAwait(false);
                LogHelper.Logger.Error($"工作流定时器连续失败 {MaxAttempts} 次，已放弃：流程 {timer.ProcessId} 置为 Terminated，定时器 {timer.Name} 不再触发");
            }
            catch (Exception ex)
            {
                LogHelper.Logger.Error($"工作流定时器放弃处理失败：流程 {timer.ProcessId}，定时器 {timer.Name}", ex);
            }
        }
    }
}
