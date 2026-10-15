using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using Microsoft.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using FluentMigrator.Runner;
using Newtonsoft.Json;
using OptimaJet.Workflow.Core;
using OptimaJet.Workflow.Core.Fault;
using OptimaJet.Workflow.Core.Model;
using OptimaJet.Workflow.Core.Persistence;
using OptimaJet.Workflow.Core.Runtime;
using OptimaJet.Workflow.Core.Runtime.Timers;
using OptimaJet.Workflow.Core.Entities;
using OptimaJet.Workflow.Migrator;
using OptimaJet.Workflow.MSSQL.Models;
using OptimaJet.Workflow.Plugins;
using WorkflowRuntime = OptimaJet.Workflow.Core.Runtime.WorkflowRuntime;
using WorkflowSync = OptimaJet.Workflow.MSSQL.Models.WorkflowSync;

// ReSharper disable once CheckNamespace
namespace OptimaJet.Workflow.DbPersistence
{
    public class MSSQLProvider : IWorkflowProvider, IMigratable
    {
        private WorkflowRuntime _runtime;
        
        private PersistenceProviderOptions Options { get; }

        /// <summary>Database schema name for workflow persistence objects</summary>
        public string SchemaName => Options.SchemaName;

        /// <summary>Global timeout for executing commands on the database</summary>
        public int GlobalCommandTimeout => Options.GlobalCommandTimeout;

        /// <summary> Connection string to the database </summary>
        public string ConnectionString => Options.ConnectionString;
        
        //Entity Providers (DbObject<TEntity>)
        public WorkflowProcessInstance WorkflowProcessInstance { get; } 
        public WorkflowProcessInstanceStatus WorkflowProcessInstanceStatus { get; }
        public WorkflowProcessInstancePersistence WorkflowProcessInstancePersistence { get; }
        public WorkflowProcessTransitionHistory WorkflowProcessTransitionHistory { get; }
        public WorkflowProcessTimer WorkflowProcessTimer { get; }
        public WorkflowInbox WorkflowInbox { get; }
        public WorkflowApprovalHistory WorkflowApprovalHistory { get; } 
        public WorkflowProcessAssignment WorkflowProcessAssignment { get; }
        public WorkflowGlobalParameter WorkflowGlobalParameter { get; }
        public WorkflowProcessScheme WorkflowProcessScheme { get; }
        public MSSQL.Models.WorkflowRuntime WorkflowRuntime { get; }
        public WorkflowScheme WorkflowScheme { get; }
        public WorkflowSync WorkflowSync { get; }
        public ProcessInstanceTree ProcessInstanceTree { get; }
        

        public virtual void Init(WorkflowRuntime runtime)
        {
            _runtime = runtime;
        }


        public MSSQLProvider(string connectionString, string schemaName = "dbo", bool writeToHistory = true,
            bool writeSubProcessToRoot = false) : this(new PersistenceProviderOptions(connectionString)
        {
            SchemaName = schemaName, WriteToHistory = writeToHistory, WriteSubProcessToRoot = writeSubProcessToRoot
        }) {}
        
        public MSSQLProvider(PersistenceProviderOptions options)
        {
            if (options == null)
            {
                throw new ArgumentException("PersistenceProviderOptions are null");
            }
            
            options.SchemaName ??= "dbo";
            Options = options;
            
            //Creating DbObjects
            WorkflowProcessInstance = new WorkflowProcessInstance(Options.SchemaName, Options.GlobalCommandTimeout);
            WorkflowProcessInstanceStatus = new WorkflowProcessInstanceStatus(Options.SchemaName, Options.GlobalCommandTimeout);
            WorkflowProcessInstancePersistence = new WorkflowProcessInstancePersistence(Options.SchemaName, Options.GlobalCommandTimeout);
            WorkflowProcessTransitionHistory = new WorkflowProcessTransitionHistory(Options.SchemaName, Options.GlobalCommandTimeout);
            WorkflowProcessTimer = new WorkflowProcessTimer(Options.SchemaName, Options.GlobalCommandTimeout);
            WorkflowInbox = new WorkflowInbox(Options.SchemaName, Options.GlobalCommandTimeout);
            WorkflowApprovalHistory = new WorkflowApprovalHistory(Options.SchemaName, Options.GlobalCommandTimeout);
            WorkflowProcessAssignment = new WorkflowProcessAssignment(Options.SchemaName, Options.GlobalCommandTimeout);
            WorkflowGlobalParameter = new WorkflowGlobalParameter(Options.SchemaName, Options.GlobalCommandTimeout);
            WorkflowProcessScheme = new WorkflowProcessScheme(Options.SchemaName, Options.GlobalCommandTimeout);
            WorkflowRuntime = new MSSQL.Models.WorkflowRuntime(Options.SchemaName, Options.GlobalCommandTimeout);
            WorkflowScheme = new WorkflowScheme(Options.SchemaName, Options.GlobalCommandTimeout);
            WorkflowSync = new WorkflowSync(Options.SchemaName, Options.GlobalCommandTimeout);
            ProcessInstanceTree = new ProcessInstanceTree(Options.SchemaName, Options.GlobalCommandTimeout);
        }

        /// <summary> Opens a new database connection </summary>
        /// <returns>A new database connection</returns>
        public SqlConnection OpenConnection()
        {
            return new SqlConnection(Options.ConnectionString);
        }

        /// <summary>
        /// abp-tx: 始终新建连接、不加入环境事务。用于 WorkflowProcessScheme：流程方案是按方案编码+参数编译出的不可变版本，
        /// 若随业务事务回滚，其他事务（ReadUncommitted 下可读到未提交行）已基于它创建的流程会引用不存在的方案（SchemeNotFoundException）。
        /// 外层回滚时最多留下未被引用的方案行，由 DropUnusedWorkflowProcessSchemeAsync 清理
        /// </summary>
        public SqlConnectionLease LeaseOwnConnection() => new SqlConnectionLease(OpenConnection(), owned: true);

        /// <summary> abp-tx: 有环境事务时借用其连接，否则新建连接（释放时关闭） </summary>
        public SqlConnectionLease LeaseConnection()
        {
            var ambient = AmbientSqlTransaction.Current;
            if (ambient == null)
            {
                return new SqlConnectionLease(OpenConnection(), owned: true);
            }

            // 环境事务连接必须指向工作流所在的库，否则工作流数据会被静默写进业务库
            var expected = new SqlConnectionStringBuilder(Options.ConnectionString).InitialCatalog;
            if (!string.Equals(ambient.Connection.Database, expected, StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException(
                    $"环境事务数据库 [{ambient.Connection.Database}] 与工作流数据库 [{expected}] 不一致，无法加入同一事务");
            }

            return new SqlConnectionLease(ambient.Connection, owned: false);
        }

        #region IPersistenceProvider
        
        #region IAssignmentProvider
        public async Task DeleteAssignmentAsync(Guid assignmentId)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            await WorkflowProcessAssignment.DeleteAsync(connection, assignmentId).ConfigureAwait(false);
        }

        public async Task<List<Assignment>> GetAssignmentsAsync(AssignmentFilter filter, List<(string parameterName,SortDirection sortDirection)> orderParameters = null, Paging paging = null)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            
            var assignments = await WorkflowProcessAssignment
                .SelectByFilterAsync(connection, filter.Parameters, orderParameters, paging).ConfigureAwait(false);
            
            return assignments.Select(a => WorkflowProcessAssignment.ConvertToAssignment(a)).ToList();
        }

        public async Task<int> GetAssignmentCountAsync(AssignmentFilter filter)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            return  await WorkflowProcessAssignment.GetAssignmentCountAsync(connection, filter.Parameters).ConfigureAwait(false);
        }

        public async Task CreateAssignmentAsync(Guid processId, AssignmentCreationForm form)
        {
            ProcessLockGuard.Write(processId); // abp-tx
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            form.Observers ??= new List<string>();
            form.Tags ??= new List<string>();
            
            var assignment = new ProcessAssignmentEntity
            {
                Id = form.Id ?? Guid.NewGuid(),
                AssignmentCode = form.AssignmentCode,
                Name = form.Name,
                Description = form.Description,
                Executor = form.Executor,
                ProcessId = processId,
                StatusState = AssignmentPlugin.DefaultStatus,
                IsDeleted = false,
                IsActive = form.IsActive,
                DeadlineToComplete = form.DeadlineToComplete,
                DeadlineToStart = form.DeadlineToStart,
                Observers = JsonConvert.SerializeObject(form.Observers),
                Tags = JsonConvert.SerializeObject(form.Tags),
                DateCreation = _runtime.RuntimeDateTimeNow
            };

            await WorkflowProcessAssignment.InsertAsync(connection, assignment).ConfigureAwait(false);
        }

        public virtual async Task<Assignment> GetAssignmentAsync(Guid assignmentId)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            
            var assignment = await WorkflowProcessAssignment.SelectByKeyAsync(connection, assignmentId).ConfigureAwait(false);
            
            return WorkflowProcessAssignment.ConvertToAssignment(assignment);
        }
        
        public async Task UpdateAssignmentAsync(Assignment a)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            
            var assignment = await WorkflowProcessAssignment.SelectByKeyAsync(connection, a.AssignmentId).ConfigureAwait(false);
            
            assignment.Name = a.Name;
            assignment.Description = a.Description;
            assignment.Executor = a.Executor;
            assignment.ProcessId = a.ProcessId;
            assignment.StatusState = a.StatusState;
            assignment.DateStart = a.DateStart;
            assignment.DateFinish = a.DateFinish;
            assignment.IsActive = a.IsActive;
            assignment.IsDeleted = a.IsDeleted;
            assignment.DeadlineToComplete = a.DeadlineToComplete;
            assignment.DeadlineToStart = a.DeadlineToStart;
            assignment.Observers = JsonConvert.SerializeObject(a.Observers ?? new List<string>());
            assignment.Tags = JsonConvert.SerializeObject(a.Tags ?? new List<string>());
            
            await WorkflowProcessAssignment.UpdateAsync(connection, assignment).ConfigureAwait(false);
        }
        #endregion
        
        public async Task DeleteInactiveTimersByProcessIdAsync(Guid processId)
        {
            ProcessLockGuard.Write(processId); // abp-tx
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            await WorkflowProcessTimer.DeleteInactiveByProcessIdAsync(connection, processId).ConfigureAwait(false);
        }

        public async Task DeleteTimerAsync(Guid timerId)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            await WorkflowProcessTimer.DeleteAsync(connection, timerId).ConfigureAwait(false);
        }

        public virtual async Task<List<Guid>> GetRunningProcessesAsync(string runtimeId = null)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            return await WorkflowProcessInstanceStatus.GetProcessesByStatusAsync(connection, ProcessStatus.Running.Id, runtimeId)
                .ConfigureAwait(false);
        }

        public virtual async Task<bool> MultiServerRuntimesExistAsync()
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            return await WorkflowRuntime.MultiServerRuntimesExistAsync(connection).ConfigureAwait(false);
        }

        public virtual async Task<int> ActiveMultiServerRuntimesCountAsync(string currentRuntimeId)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            return await WorkflowRuntime.GetActiveMultiServerRuntimesCountAsync(connection, currentRuntimeId).ConfigureAwait(false);
        }

        public virtual async Task<WorkflowRuntimeModel> CreateWorkflowRuntimeAsync(string runtimeId, RuntimeStatus status)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            
            var runtime = new RuntimeEntity
            {
                RuntimeId = runtimeId,
                Lock = Guid.NewGuid(),
                Status = status
            };
            
            await WorkflowRuntime.InsertAsync(connection, runtime).ConfigureAwait(false);

            return new WorkflowRuntimeModel { Lock = runtime.Lock, RuntimeId = runtimeId, Status = status };
        }

        public virtual async Task DeleteWorkflowRuntimeAsync(string name)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            await WorkflowRuntime.DeleteAsync(connection, name).ConfigureAwait(false);
        }

        public async Task<List<ProcessInstanceItem>> GetProcessInstancesAsync(
            List<(string parameterName, SortDirection sortDirection)> orderParameters = null,
            Paging paging = null)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            
            ProcessInstanceEntity[] processInstances = await WorkflowProcessInstance
                .SelectAllWithPagingAsync(connection, orderParameters, paging).ConfigureAwait(false);
           
            return processInstances.Select(pi => new ProcessInstanceItem()
            {
                ActivityName  = pi.ActivityName,
                Id  = pi.Id,
                IsDeterminingParametersChanged  = pi.IsDeterminingParametersChanged,
                PreviousActivity  = pi.PreviousActivity,
                PreviousActivityForDirect  = pi.PreviousActivityForDirect,
                PreviousActivityForReverse  = pi.PreviousActivityForReverse,
                PreviousState  = pi.PreviousState,
                PreviousStateForDirect  = pi.PreviousStateForDirect,
                PreviousStateForReverse  = pi.PreviousStateForReverse,
                SchemeId  = pi.SchemeId,
                StateName  = pi.StateName,
                ParentProcessId  = pi.ParentProcessId,
                RootProcessId  = pi.RootProcessId,
                TenantId  = pi.TenantId,
                StartingTransition  = pi.StartingTransition,
                SubprocessName  = pi.SubprocessName,
                CreationDate  = pi.CreationDate,
                LastTransitionDate  = pi.LastTransitionDate,
                CalendarName = pi.CalendarName
            }).ToList();
        }

        public async Task<int> GetProcessInstancesCountAsync()
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            return await WorkflowProcessInstance.GetCountAsync(connection).ConfigureAwait(false);
        }

        public async Task<List<SchemeItem>> GetSchemesAsync(List<(string parameterName,SortDirection sortDirection)> orderParameters = null, Paging paging = null)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            
            var schemes = await WorkflowScheme.SelectAllWorkflowSchemesWithPagingAsync(connection, orderParameters, paging)
                .ConfigureAwait(false);
           
            return schemes.Select(sc => new SchemeItem()
            {
                Code = sc.Code,
                Scheme = sc.Scheme,
                CanBeInlined = sc.CanBeInlined,
                InlinedSchemes = sc.GetInlinedSchemes(),
                Tags = TagHelper.FromTagString(sc.Tags),
            }).ToList();
        }
       
        public async Task<int> GetSchemesCountAsync()
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            return await WorkflowScheme.GetCountAsync(connection).ConfigureAwait(false);
        }

        public virtual async Task<WorkflowRuntimeModel> UpdateWorkflowRuntimeStatusAsync(WorkflowRuntimeModel runtime, RuntimeStatus status)
        {
            Tuple<int, WorkflowRuntimeModel> res = await UpdateWorkflowRuntimeAsync(runtime,
                    x => x.Status = status,
                    WorkflowRuntime.UpdateStatusAsync)
                .ConfigureAwait(false);

            if (res.Item1 != 1)
            {
                throw new ImpossibleToSetRuntimeStatusException();
            }

            return res.Item2;
        }

        public virtual async Task<(bool Success, WorkflowRuntimeModel UpdatedModel)> UpdateWorkflowRuntimeRestorerAsync(WorkflowRuntimeModel runtime, string restorerId)
        {
            Tuple<int, WorkflowRuntimeModel> res = await UpdateWorkflowRuntimeAsync(runtime,
                    x => x.RestorerId = restorerId,
                    WorkflowRuntime.UpdateRestorerAsync)
                .ConfigureAwait(false);

            return (res.Item1 == 1, res.Item2);
        }

        public virtual async Task InitializeProcessAsync(ProcessInstance processInstance)
        {
            ProcessLockGuard.Write(processInstance.ProcessId); // abp-tx
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            
            ProcessInstanceEntity oldProcess = await WorkflowProcessInstance.SelectByKeyAsync(connection, processInstance.ProcessId)
                .ConfigureAwait(false);
            
            if (oldProcess != null)
            {
                throw new ProcessAlreadyExistsException(processInstance.ProcessId);
            }
            
            var newProcess = new ProcessInstanceEntity
            {
                Id = processInstance.ProcessId,
                SchemeId = processInstance.SchemeId,
                ActivityName = processInstance.ProcessScheme.InitialActivity.Name,
                StateName = processInstance.ProcessScheme.InitialActivity.State,
                RootProcessId = processInstance.RootProcessId,
                ParentProcessId = processInstance.ParentProcessId,
                TenantId = processInstance.TenantId,
                StartingTransition = processInstance.ProcessScheme.StartingTransition,
                SubprocessName = processInstance.SubprocessName,
                CreationDate = processInstance.CreationDate,
                CalendarName = processInstance.CalendarName
            };
            
            await WorkflowProcessInstance.InsertAsync(connection, newProcess).ConfigureAwait(false);
        }

        public virtual async Task BindProcessToNewSchemeAsync(ProcessInstance processInstance)
        {
            ProcessLockGuard.Write(processInstance.ProcessId); // abp-tx
            await BindProcessToNewSchemeAsync(processInstance, false).ConfigureAwait(false);
        }

        public virtual async Task BindProcessToNewSchemeAsync(ProcessInstance processInstance, bool resetIsDeterminingParametersChanged)
        {
            ProcessLockGuard.Write(processInstance.ProcessId); // abp-tx
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            
            ProcessInstanceEntity oldProcess = await WorkflowProcessInstance.SelectByKeyAsync(connection, processInstance.ProcessId)
                .ConfigureAwait(false);
            
            if (oldProcess == null)
            {
                throw new ProcessNotFoundException(processInstance.ProcessId);
            }
            
            oldProcess.SchemeId = processInstance.SchemeId;
            oldProcess.StartingTransition = processInstance.ProcessScheme.StartingTransition;
            
            if (resetIsDeterminingParametersChanged)
            {
                oldProcess.IsDeterminingParametersChanged = false;
            }

            await WorkflowProcessInstance.UpdateAsync(connection, oldProcess).ConfigureAwait(false);
        }

        public virtual async Task FillProcessParametersAsync(ProcessInstance processInstance)
        {
            ProcessLockGuard.Read(processInstance.ProcessId); // abp-tx
            processInstance.AddParameters(await GetProcessParametersAsync(processInstance.ProcessId, processInstance.ProcessScheme).ConfigureAwait(false));
        }

        public virtual async Task FillPersistedProcessParametersAsync(ProcessInstance processInstance)
        {
            ProcessLockGuard.Read(processInstance.ProcessId); // abp-tx
            processInstance.AddParameters(await GetPersistedProcessParametersAsync(processInstance.ProcessId, processInstance.ProcessScheme).ConfigureAwait(false));
        }

        public virtual async Task FillPersistedProcessParameterAsync(ProcessInstance processInstance, string parameterName)
        {
            ProcessLockGuard.Read(processInstance.ProcessId); // abp-tx
            ParameterDefinitionWithValue persistedProcessParameter = await GetPersistedProcessParameterAsync(processInstance.ProcessId, processInstance.ProcessScheme, parameterName).ConfigureAwait(false);
            if (persistedProcessParameter == null)
            {
                return;
            }
            processInstance.AddParameter(persistedProcessParameter);
        }

        public virtual async Task FillSystemProcessParametersAsync(ProcessInstance processInstance)
        {
            ProcessLockGuard.Read(processInstance.ProcessId); // abp-tx
            processInstance.AddParameters(await GetSystemProcessParametersAsync(processInstance.ProcessId, processInstance.ProcessScheme).ConfigureAwait(false));
        }

        public virtual async Task SavePersistenceParametersAsync(ProcessInstance processInstance)
        {
            ProcessLockGuard.Write(processInstance.ProcessId); // abp-tx
            var parametersToPersistList = processInstance.ProcessParameters.Where(ptp => ptp.Purpose == ParameterPurpose.Persistence)
                                                                           .Select(ptp => ParameterDefinitionWithValueToDynamic(ptp))
                                                                           .ToList();
            
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx

            var persistedParameters = 
                (await WorkflowProcessInstancePersistence.SelectByProcessIdAsync(connection, processInstance.ProcessId)
                    .ConfigureAwait(false)).ToList();

            foreach (dynamic parameterDefinitionWithValue in parametersToPersistList)
            {
                var persistence = persistedParameters.SingleOrDefault(pp => pp.ParameterName == parameterDefinitionWithValue.Parameter.Name);

                await InsertOrUpdateParameterAsync(connection, processInstance, parameterDefinitionWithValue, persistence)
                    .ConfigureAwait(false);
            }
        }
        public virtual async Task SavePersistenceParameterAsync(ProcessInstance processInstance, string parameterName)
        {
            ProcessLockGuard.Write(processInstance.ProcessId); // abp-tx
            dynamic parameter = ParameterDefinitionWithValueToDynamic(processInstance.ProcessParameters.Single(ptp => ptp.Purpose == ParameterPurpose.Persistence && ptp.Name == parameterName));

            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            
            var persistedParameter = await WorkflowProcessInstancePersistence
                .SelectByNameAsync(connection, processInstance.ProcessId, parameterName).ConfigureAwait(false);

            await InsertOrUpdateParameterAsync(connection, processInstance, parameter, persistedParameter).ConfigureAwait(false);
        }
        
        private dynamic ParameterDefinitionWithValueToDynamic(ParameterDefinitionWithValue ptp)
        {
            string serializedValue = ptp.Type == typeof(UnknownParameterType) ? (string)ptp.Value : ParametersSerializer.Serialize(ptp.Value, ptp.Type);
            return new { Parameter = ptp, SerializedValue = serializedValue };
        }

        private async Task InsertOrUpdateParameterAsync(SqlConnection connection, ProcessInstance processInstance, dynamic parameter,
            ProcessInstancePersistenceEntity persistence)
        {
            if (persistence == null)
            {
                if (parameter.SerializedValue != null)
                {
                    persistence = new ProcessInstancePersistenceEntity()
                    {
                        Id = Guid.NewGuid(),
                        ProcessId = processInstance.ProcessId,
                        ParameterName = parameter.Parameter.Name,
                        Value = parameter.SerializedValue
                    };
                    
                    await WorkflowProcessInstancePersistence.InsertAsync(connection, persistence).ConfigureAwait(false);
                }
            }
            else
            {
                if (parameter.SerializedValue != null)
                {
                    persistence.Value = parameter.SerializedValue;
                    await WorkflowProcessInstancePersistence.UpdateAsync(connection, persistence).ConfigureAwait(false);
                }
                else
                {
                    await WorkflowProcessInstancePersistence.DeleteAsync(connection, persistence.Id).ConfigureAwait(false);
                }
            }
        }
        
        public virtual async Task RemoveParameterAsync(ProcessInstance processInstance, string parameterName)
        {
            ProcessLockGuard.Write(processInstance.ProcessId); // abp-tx
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            await WorkflowProcessInstancePersistence.DeleteByNameAsync(connection, processInstance.ProcessId, parameterName)
                .ConfigureAwait(false);
        }

        public virtual async Task SetProcessStatusAsync(Guid processId, ProcessStatus newStatus)
        {
            ProcessLockGuard.Write(processId); // abp-tx
            if (newStatus == ProcessStatus.Running)
            {
                await SetRunningStatusAsync(processId).ConfigureAwait(false);
            }
            else
            {
                await SetCustomStatusAsync(processId,newStatus).ConfigureAwait(false);
            }
        }

        public virtual async Task SetWorkflowInitializedAsync(ProcessInstance processInstance)
        {
            ProcessLockGuard.Write(processInstance.ProcessId); // abp-tx
            await SetCustomStatusAsync(processInstance.ProcessId, ProcessStatus.Initialized, true).ConfigureAwait(false);
        }

        public virtual async Task SetWorkflowIdledAsync(ProcessInstance processInstance)
        {
            ProcessLockGuard.Write(processInstance.ProcessId); // abp-tx
            await SetCustomStatusAsync(processInstance.ProcessId, ProcessStatus.Idled).ConfigureAwait(false);
        }

        public virtual async Task SetWorkflowRunningAsync(ProcessInstance processInstance)
        {
            ProcessLockGuard.Write(processInstance.ProcessId); // abp-tx
            Guid processId = processInstance.ProcessId;
            await SetRunningStatusAsync(processId).ConfigureAwait(false);
        }

        public virtual async Task SetWorkflowFinalizedAsync(ProcessInstance processInstance)
        {
            ProcessLockGuard.Write(processInstance.ProcessId); // abp-tx
            await SetCustomStatusAsync(processInstance.ProcessId, ProcessStatus.Finalized).ConfigureAwait(false);
        }

        public virtual async Task SetWorkflowTerminatedAsync(ProcessInstance processInstance)
        {
            ProcessLockGuard.Write(processInstance.ProcessId); // abp-tx
            await SetCustomStatusAsync(processInstance.ProcessId, ProcessStatus.Terminated).ConfigureAwait(false);
        }

        public async Task WriteInitialRecordToHistoryAsync(ProcessInstance processInstance)
        {
            ProcessLockGuard.Write(processInstance.ProcessId); // abp-tx
            if (!Options.WriteToHistory) { return; }

            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx

            var history = new ProcessTransitionHistoryEntity
            {
                Id = Guid.NewGuid(),
                ProcessId = Options.WriteSubProcessToRoot && processInstance.IsSubprocess
                    ? processInstance.RootProcessId
                    : processInstance.ProcessId,
                FromActivityName = String.Empty,
                FromStateName = String.Empty,
                ToActivityName = processInstance.CurrentActivityName,
                ToStateName = processInstance.CurrentState,
                TransitionClassifier = nameof(TransitionClassifier.NotSpecified),
                TransitionTime = _runtime.RuntimeDateTimeNow,
                TriggerName = "Initializing",
                StartTransitionTime = _runtime.RuntimeDateTimeNow,
                TransitionDuration = 0
            };

            await WorkflowProcessTransitionHistory.InsertAsync(connection, history).ConfigureAwait(false);
        }

        public virtual async Task UpdatePersistenceStateAsync(ProcessInstance processInstance, TransitionDefinition transition)
        {
            ProcessLockGuard.Write(processInstance.ProcessId); // abp-tx
            DateTime startTransitionTime = processInstance.StartTransitionTime ?? _runtime.RuntimeDateTimeNow;

            ParameterDefinitionWithValue paramIdentityId = await processInstance.GetParameterAsync(DefaultDefinitions.ParameterIdentityId.Name).ConfigureAwait(false);
            ParameterDefinitionWithValue paramImpIdentityId = await processInstance.GetParameterAsync(DefaultDefinitions.ParameterImpersonatedIdentityId.Name).ConfigureAwait(false);

            string identityId = paramIdentityId == null ? String.Empty : (string)paramIdentityId.Value;
            string impIdentityId = paramImpIdentityId == null ? identityId : (string)paramImpIdentityId.Value;

            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            
            var instance = await WorkflowProcessInstance.SelectByKeyAsync(connection, processInstance.ProcessId).ConfigureAwait(false);

            if (!(instance == null || transition.To.DisablePersistState))
            {
                if (!String.IsNullOrEmpty(transition.To.State))
                {
                    instance.StateName = transition.To.State;
                }

                instance.ActivityName = transition.To.Name;
                instance.PreviousActivity = transition.From.Name;

                if (!String.IsNullOrEmpty(transition.From.State))
                {
                    instance.PreviousState = transition.From.State;
                }

                if (transition.Classifier == TransitionClassifier.Direct)
                {
                    instance.PreviousActivityForDirect = transition.From.Name;

                    if (!String.IsNullOrEmpty(transition.From.State))
                    {
                        instance.PreviousStateForDirect = transition.From.State;
                    }
                }
                else if (transition.Classifier == TransitionClassifier.Reverse)
                {
                    instance.PreviousActivityForReverse = transition.From.Name;

                    if (!String.IsNullOrEmpty(transition.From.State))
                    {
                        instance.PreviousStateForReverse = transition.From.State;
                    }
                }

                instance.ParentProcessId = processInstance.ParentProcessId;
                instance.RootProcessId = processInstance.RootProcessId;
                instance.LastTransitionDate = processInstance.LastTransitionDate;

                await WorkflowProcessInstance.UpdateAsync(connection, instance).ConfigureAwait(false);
            }

            if (!Options.WriteToHistory || transition.To.DisablePersistTransitionHistory)
            {
                return;
            }

            string actorName = null;
            string executorName = null;
            if (_runtime.GetUserByIdentityAsync != null)
            {
                if (!String.IsNullOrEmpty(impIdentityId) )
                {
                    actorName = await _runtime.GetUserByIdentityAsync(impIdentityId).ConfigureAwait(false);
                }
                if (!String.IsNullOrEmpty(identityId))
                {
                    executorName = await _runtime.GetUserByIdentityAsync(identityId).ConfigureAwait(false);
                }
            }

            var history = new ProcessTransitionHistoryEntity
            {
                ActorIdentityId = impIdentityId,
                ExecutorIdentityId = identityId,
                ActorName = actorName,
                ExecutorName = executorName,
                Id = Guid.NewGuid(),
                IsFinalised = transition.To.IsFinal,
                ProcessId = Options.WriteSubProcessToRoot && processInstance.IsSubprocess ? processInstance.RootProcessId : processInstance.ProcessId,
                FromActivityName = transition.From.Name,
                FromStateName = transition.From.State,
                ToActivityName = transition.To.Name,
                ToStateName = transition.To.State,
                TransitionClassifier = transition.Classifier.ToString(),
                TransitionTime = _runtime.RuntimeDateTimeNow,
                TriggerName = String.IsNullOrEmpty(processInstance.ExecutedTimer) ? processInstance.CurrentCommand : processInstance.ExecutedTimer,
                StartTransitionTime = startTransitionTime,
                TransitionDuration = (int)(_runtime.RuntimeDateTimeNow - startTransitionTime).TotalMilliseconds
            };
            
            await WorkflowProcessTransitionHistory.InsertAsync(connection, history).ConfigureAwait(false);
        }

        public virtual async Task<bool> IsProcessExistsAsync(Guid processId)
        {
            ProcessLockGuard.Read(processId); // abp-tx
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            return await WorkflowProcessInstance.SelectByKeyAsync(connection, processId).ConfigureAwait(false) != null;
        }

        public virtual async Task<ProcessStatus> GetInstanceStatusAsync(Guid processId)
        {
            ProcessLockGuard.Read(processId); // abp-tx
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            
            var instanceStatus = await WorkflowProcessInstanceStatus.SelectByKeyAsync(connection, processId).ConfigureAwait(false);
            
            if (instanceStatus == null)
            {
                return ProcessStatus.NotFound;
            }
            
            ProcessStatus status = ProcessStatus.All.SingleOrDefault(ins => ins.Id == instanceStatus.Status);
            
            return status ?? ProcessStatus.Unknown;
        }
        
        private async Task SetRunningStatusAsync(Guid processId)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            
            var instanceStatus = await WorkflowProcessInstanceStatus.SelectByKeyAsync(connection, processId).ConfigureAwait(false);

            if (instanceStatus == null)
            {
                throw new StatusNotDefinedException();
            }

            if (instanceStatus.Status == ProcessStatus.Running.Id)
            {
                throw new ImpossibleToSetStatusException("Process already running");
            }

            Guid oldLock = instanceStatus.Lock;

            instanceStatus.Lock = Guid.NewGuid();
            instanceStatus.Status = ProcessStatus.Running.Id;
            instanceStatus.RuntimeId = _runtime.Id;
            instanceStatus.SetTime = _runtime.RuntimeDateTimeNow;
            
            int cnt = await WorkflowProcessInstanceStatus.ChangeStatusAsync(connection, instanceStatus, oldLock).ConfigureAwait(false);
            
            if (cnt == 0)
            {
                instanceStatus = await WorkflowProcessInstanceStatus.SelectByKeyAsync(connection, processId).ConfigureAwait(false);
                
                if (instanceStatus == null)
                {
                    throw new StatusNotDefinedException();
                }
            }

            if (cnt != 1)
            {
                throw new ImpossibleToSetStatusException();
            }
        }

        // ReSharper disable once ParameterOnlyUsedForPreconditionCheck.Local
        private async Task SetCustomStatusAsync(Guid processId, ProcessStatus status, bool createIfNotDefined = false)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            
            var instanceStatus = await WorkflowProcessInstanceStatus.SelectByKeyAsync(connection, processId).ConfigureAwait(false);
            
            if (instanceStatus == null)
            {
                if (!createIfNotDefined)
                {
                    throw new StatusNotDefinedException();
                }

                instanceStatus = new ProcessInstanceStatusEntity()
                {
                    Id = processId,
                    Lock = Guid.NewGuid(),
                    Status = status.Id,
                    RuntimeId = _runtime.Id,
                    SetTime = _runtime.RuntimeDateTimeNow
                };

                await WorkflowProcessInstanceStatus.InsertAsync(connection, instanceStatus).ConfigureAwait(false);
            }
            else
            {
                Guid oldLock = instanceStatus.Lock;

                instanceStatus.Status = status.Id;
                instanceStatus.Lock = Guid.NewGuid();
                instanceStatus.RuntimeId = _runtime.Id;
                instanceStatus.SetTime = _runtime.RuntimeDateTimeNow;

                int cnt = await WorkflowProcessInstanceStatus.ChangeStatusAsync(connection, instanceStatus, oldLock).ConfigureAwait(false);

                if (cnt == 0)
                {
                    instanceStatus = await WorkflowProcessInstanceStatus.SelectByKeyAsync(connection, processId).ConfigureAwait(false);
                    
                    if (instanceStatus == null)
                    {
                        throw new StatusNotDefinedException();
                    }
                }

                if (cnt != 1)
                {
                    throw new ImpossibleToSetStatusException();
                }
            }
        }

        private async Task<List<ParameterDefinitionWithValue>> GetProcessParametersAsync(Guid processId, ProcessDefinition processDefinition)
        {
            var parameters = new List<ParameterDefinitionWithValue>(processDefinition.Parameters.Count);
            parameters.AddRange(await GetPersistedProcessParametersAsync(processId, processDefinition).ConfigureAwait(false));
            parameters.AddRange(await GetSystemProcessParametersAsync(processId, processDefinition).ConfigureAwait(false));
            return parameters;
        }

        private async Task<List<ParameterDefinitionWithValue>> GetSystemProcessParametersAsync(Guid processId, ProcessDefinition processDefinition)
        {
            var processInstance = await GetProcessInstanceAsync(processId).ConfigureAwait(false);

            var systemParameters =
                processDefinition.Parameters.Where(p => p.Purpose == ParameterPurpose.System).ToList();

            var parameters = new List<ParameterDefinitionWithValue>(systemParameters.Count)
            {
                ParameterDefinition.Create(
                    systemParameters.Single(sp => sp.Name == DefaultDefinitions.ParameterProcessId.Name),
                    processId),
                ParameterDefinition.Create(
                    systemParameters.Single(sp => sp.Name == DefaultDefinitions.ParameterPreviousState.Name),
                    processInstance.PreviousState),
                ParameterDefinition.Create(
                    systemParameters.Single(sp => sp.Name == DefaultDefinitions.ParameterCurrentState.Name),
                    processInstance.StateName),
                ParameterDefinition.Create(
                    systemParameters.Single(sp => sp.Name == DefaultDefinitions.ParameterPreviousStateForDirect.Name),
                    processInstance.PreviousStateForDirect),
                ParameterDefinition.Create(
                    systemParameters.Single(sp => sp.Name == DefaultDefinitions.ParameterPreviousStateForReverse.Name),
                    processInstance.PreviousStateForReverse),
                ParameterDefinition.Create(
                    systemParameters.Single(sp => sp.Name == DefaultDefinitions.ParameterPreviousActivity.Name),
                    processInstance.PreviousActivity),
                ParameterDefinition.Create(
                    systemParameters.Single(sp => sp.Name == DefaultDefinitions.ParameterCurrentActivity.Name),
                    processInstance.ActivityName),
                ParameterDefinition.Create(
                    systemParameters.Single(sp => sp.Name == DefaultDefinitions.ParameterPreviousActivityForDirect.Name),
                    processInstance.PreviousActivityForDirect),
                ParameterDefinition.Create(
                    systemParameters.Single(sp => sp.Name == DefaultDefinitions.ParameterPreviousActivityForReverse.Name),
                    processInstance.PreviousActivityForReverse),
                ParameterDefinition.Create(
                    systemParameters.Single(sp => sp.Name == DefaultDefinitions.ParameterSchemeCode.Name),
                    processDefinition.Name),
                ParameterDefinition.Create(
                    systemParameters.Single(sp => sp.Name == DefaultDefinitions.ParameterSchemeId.Name),
                    processInstance.SchemeId),
                ParameterDefinition.Create(
                    systemParameters.Single(sp => sp.Name == DefaultDefinitions.ParameterIsPreExecution.Name),
                    false),
                ParameterDefinition.Create(
                    systemParameters.Single(sp => sp.Name == DefaultDefinitions.ParameterParentProcessId.Name),
                    processInstance.ParentProcessId),
                ParameterDefinition.Create(
                    systemParameters.Single(sp => sp.Name == DefaultDefinitions.ParameterRootProcessId.Name),
                    processInstance.RootProcessId),
                ParameterDefinition.Create(
                    systemParameters.Single(sp => sp.Name == DefaultDefinitions.ParameterTenantId.Name),
                    processInstance.TenantId),
                ParameterDefinition.Create(
                    systemParameters.Single(sp => sp.Name == DefaultDefinitions.ParameterSubprocessName.Name),
                    processInstance.SubprocessName),
                ParameterDefinition.Create(
                    systemParameters.Single(sp => sp.Name == DefaultDefinitions.ParameterCreationDate.Name),
                    processInstance.CreationDate),
                ParameterDefinition.Create(
                    systemParameters.Single(sp => sp.Name == DefaultDefinitions.ParameterLastTransitionDate.Name),
                    processInstance.LastTransitionDate),
                ParameterDefinition.Create(
                    systemParameters.Single(sp => sp.Name == DefaultDefinitions.ParameterCalendarName.Name),
                    processInstance.CalendarName)
            };
            return parameters;
        }

        private async Task<List<ParameterDefinitionWithValue>> GetPersistedProcessParametersAsync(Guid processId, ProcessDefinition processDefinition)
        {
            var persistenceParameters = processDefinition.PersistenceParameters.ToList();
            var parameters = new List<ParameterDefinitionWithValue>(persistenceParameters.Count);

            List<ProcessInstancePersistenceEntity> persistedParameters;
            
            using (var connectionLease = LeaseConnection())
            {
                var connection = connectionLease.Connection; // abp-tx
                persistedParameters = (await WorkflowProcessInstancePersistence.SelectByProcessIdAsync(connection, processId).ConfigureAwait(false)).ToList();
            }

            foreach (var persistedParameter in persistedParameters)
            {
                parameters.Add(WorkflowProcessInstancePersistenceToParameterDefinitionWithValue(persistenceParameters, persistedParameter));
            }

            return parameters;
        }

        private async Task<ParameterDefinitionWithValue> GetPersistedProcessParameterAsync(Guid processId, ProcessDefinition processDefinition, string parameterName)
        {
            var persistenceParameters = processDefinition.PersistenceParameters.ToList();
            
            ProcessInstancePersistenceEntity persistedParameter;
            
            using (var connectionLease = LeaseConnection())
            {
                var connection = connectionLease.Connection; // abp-tx
                persistedParameter = await WorkflowProcessInstancePersistence.SelectByNameAsync(connection, processId, parameterName)
                    .ConfigureAwait(false);
            }

            return persistedParameter == null
                ? null
                : WorkflowProcessInstancePersistenceToParameterDefinitionWithValue(persistenceParameters, persistedParameter);
        }

        private ParameterDefinitionWithValue WorkflowProcessInstancePersistenceToParameterDefinitionWithValue(
            List<ParameterDefinition> persistenceParameters, ProcessInstancePersistenceEntity persistedParameter)
        {
            ParameterDefinition parameterDefinition = persistenceParameters.FirstOrDefault(p => p.Name == persistedParameter.ParameterName);
            
            if (parameterDefinition == null)
            {
                parameterDefinition = ParameterDefinition.Create(persistedParameter.ParameterName, typeof(UnknownParameterType),
                    ParameterPurpose.Persistence);
                
                return ParameterDefinition.Create(parameterDefinition, persistedParameter.Value);
            }

            return ParameterDefinition.Create(parameterDefinition, ParametersSerializer.Deserialize(persistedParameter.Value, parameterDefinition.Type));
        }

        private async Task<ProcessInstanceEntity> GetProcessInstanceAsync(Guid processId)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            
            var processInstance = await WorkflowProcessInstance.SelectByKeyAsync(connection, processId).ConfigureAwait(false);
            
            if (processInstance == null)
            {
                throw new ProcessNotFoundException(processId);
            }
            
            return processInstance;
        }

        public virtual async Task DeleteProcessAsync(Guid[] processIds)
        {
            foreach (var id in processIds) ProcessLockGuard.Write(id); // abp-tx
            foreach (Guid processId in processIds)
            {
                await DeleteProcessAsync(processId).ConfigureAwait(false);
            }
        }

        public virtual async Task SaveGlobalParameterAsync<T>(string type, string name, T value)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            
            var globalParameter = (await WorkflowGlobalParameter.SelectByTypeAndNameAsync(connection, type, name).ConfigureAwait(false))
                .FirstOrDefault();

            if (globalParameter == null)
            {
                globalParameter = new GlobalParameterEntity()
                {
                    Id = Guid.NewGuid(),
                    Type = type,
                    Name = name,
                    Value = JsonConvert.SerializeObject(value)
                };
                
                await WorkflowGlobalParameter.InsertAsync(connection, globalParameter).ConfigureAwait(false);
            }
            else
            {
                globalParameter.Value = JsonConvert.SerializeObject(value);

                await WorkflowGlobalParameter.UpdateAsync(connection, globalParameter).ConfigureAwait(false);
            }
        }

        public virtual async Task<T> LoadGlobalParameterAsync<T>(string type, string name)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            
            var globalParameter = (await WorkflowGlobalParameter.SelectByTypeAndNameAsync(connection, type, name).ConfigureAwait(false))
                .FirstOrDefault();

            return globalParameter == null
                ? default
                : JsonConvert.DeserializeObject<T>(globalParameter.Value);
        }

        public async Task<Dictionary<string, T>> LoadGlobalParametersWithNamesAsync<T>(string type, Sorting sort = null)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            
            var parameters = await WorkflowGlobalParameter.SelectByTypeAndNameAsync(connection, type, null, sort).ConfigureAwait(false);

            var dict = new Dictionary<string, T>();
            foreach (var parameter in parameters)
            {
                dict[parameter.Name] = JsonConvert.DeserializeObject<T>(parameter.Value);
            }

            return dict;
        }
        
        public virtual async Task<List<T>> LoadGlobalParametersAsync<T>(string type, Sorting sort = null)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            
            var parameters = await WorkflowGlobalParameter.SelectByTypeAndNameAsync(connection, type, null, sort).ConfigureAwait(false);

            return parameters.Select(p => JsonConvert.DeserializeObject<T>(p.Value)).ToList();
        }

        public virtual async Task<PagedResponse<T>> LoadGlobalParametersWithPagingAsync<T>(string type, Paging paging, string name = null, Sorting sort = null)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            var parameters = await WorkflowGlobalParameter
                .SearchByTypeAndNameWithPagingAsync(connection, type, name, paging, sort)
                .ConfigureAwait(false);
            var count = await WorkflowGlobalParameter.GetCountByTypeAndNameAsync(connection, type, name)
                .ConfigureAwait(false);
            return new PagedResponse<T>()
            {
                Data = parameters.Select(p => JsonConvert.DeserializeObject<T>(p.Value)).ToList(),
                Count = count
            };
        }
        
        public virtual async Task DeleteGlobalParametersAsync(string type, string name = null)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            await WorkflowGlobalParameter.DeleteByTypeAndNameAsync(connection, type, name).ConfigureAwait(false);
        }

        public virtual async Task DeleteProcessAsync(Guid processId)
        {
            ProcessLockGuard.Write(processId); // abp-tx
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            // abp-tx: LocalSqlTransaction.Begin 负责打开连接
            using var localTx = LocalSqlTransaction.Begin(connection); var transaction = localTx.Transaction; // abp-tx
            
            await WorkflowProcessInstance.DeleteAsync(connection, processId, transaction).ConfigureAwait(false);
            await WorkflowProcessInstanceStatus.DeleteAsync(connection, processId, transaction).ConfigureAwait(false);
            await WorkflowProcessInstancePersistence.DeleteByProcessIdAsync(connection, processId, transaction).ConfigureAwait(false);
            await WorkflowProcessTransitionHistory.DeleteByProcessIdAsync(connection, processId, transaction).ConfigureAwait(false);
            await WorkflowProcessTimer.DeleteByProcessIdAsync(connection, processId, null, transaction).ConfigureAwait(false);
            await WorkflowInbox.DeleteByProcessIdAsync(connection, processId, transaction).ConfigureAwait(false);
            await WorkflowApprovalHistory.DeleteByProcessIdAsync(connection, processId, transaction).ConfigureAwait(false);
            await WorkflowProcessAssignment.DeleteByProcessIdAsync(connection, processId, transaction).ConfigureAwait(false);
            
            localTx.Commit(); // abp-tx
        }

        public virtual async Task RegisterTimerAsync(Guid processId, Guid rootProcessId, string name, DateTime nextExecutionDateTime, bool notOverrideIfExists)
        {
            ProcessLockGuard.Write(processId); // abp-tx
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            
            var timer = await WorkflowProcessTimer.SelectByProcessIdAndNameAsync(connection, processId, name).ConfigureAwait(false);
            
            if (timer == null)
            {
                timer = new ProcessTimerEntity
                {
                    Id = Guid.NewGuid(),
                    Name = name,
                    NextExecutionDateTime = nextExecutionDateTime,
                    ProcessId = processId,
                    Ignore = false,
                    RootProcessId = rootProcessId
                };

                await WorkflowProcessTimer.InsertAsync(connection, timer).ConfigureAwait(false);
            }
            else if (!notOverrideIfExists)
            {
                timer.NextExecutionDateTime = nextExecutionDateTime;
                await WorkflowProcessTimer.UpdateAsync(connection, timer).ConfigureAwait(false);
            }
        }

        public virtual async Task ClearTimersAsync(Guid processId, List<string> timersIgnoreList)
        {
            ProcessLockGuard.Write(processId); // abp-tx
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            await WorkflowProcessTimer.DeleteByProcessIdAsync(connection, processId, timersIgnoreList).ConfigureAwait(false);
        }

        public virtual async Task<int> SetTimerIgnoreAsync(Guid timerId)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            return await WorkflowProcessTimer.SetTimerIgnoreAsync(connection, timerId).ConfigureAwait(false);
        }

        /// <summary> abp-tx: 取消定时器的 Ignore 标记，使其重新可被执行 </summary>
        public virtual async Task<int> ClearTimerIgnoreAsync(Guid timerId)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection;
            return await WorkflowProcessTimer.ClearTimerIgnoreAsync(connection, timerId).ConfigureAwait(false);
        }

        public virtual async Task<List<Core.Model.WorkflowTimer>> GetTopTimersToExecuteAsync(int top)
        {
            DateTime now = _runtime.RuntimeDateTimeNow;

            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            ProcessTimerEntity[] timers = await WorkflowProcessTimer.GetTopTimersToExecuteAsync(connection, top, now).ConfigureAwait(false);

            if (timers.Length == 0)
            {
                return new List<Core.Model.WorkflowTimer>();
            }

            return timers.Select(t => new Core.Model.WorkflowTimer()
            { 
                Name = t.Name,
                ProcessId = t.ProcessId,
                TimerId = t.Id,
                NextExecutionDateTime = t.NextExecutionDateTime,
                RootProcessId = t.RootProcessId,
            }).ToList();
        }

       public virtual async Task<List<ProcessHistoryItem>> GetProcessHistoryAsync(Guid processId, Paging paging = null)
        {
            ProcessLockGuard.Read(processId); // abp-tx
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            
            return (await WorkflowProcessTransitionHistory.SelectByProcessIdAsync(connection, processId, paging).ConfigureAwait(false))
                .Select(hi => new ProcessHistoryItem
                {
                    ActorIdentityId = hi.ActorIdentityId,
                    ExecutorIdentityId = hi.ExecutorIdentityId,
                    ActorName = hi.ActorName,
                    ExecutorName = hi.ExecutorName,
                    FromActivityName = hi.FromActivityName,
                    FromStateName = hi.FromStateName,
                    IsFinalised = hi.IsFinalised,
                    ProcessId = hi.ProcessId,
                    ToActivityName = hi.ToActivityName,
                    ToStateName = hi.ToStateName,
                    TransitionClassifier = (TransitionClassifier) Enum.Parse(typeof(TransitionClassifier), hi.TransitionClassifier),
                    TransitionTime = hi.TransitionTime,
                    TriggerName = hi.TriggerName,
                    StartTransitionTime = hi.StartTransitionTime,
                    TransitionDuration = hi.TransitionDuration
                })
                .ToList();
        }

       public async Task<int> GetProcessHistoryCountAsync(Guid processId)
       {
           ProcessLockGuard.Read(processId); // abp-tx
           using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
           return await WorkflowProcessTransitionHistory.GetCountByAsync(connection, x => x.ProcessId, processId)
               .ConfigureAwait(false);
       }

       public virtual async Task<List<ProcessTimer>> GetTimersForProcessAsync(Guid processId)
        {
            ProcessLockGuard.Read(processId); // abp-tx
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            
            IEnumerable<ProcessTimerEntity> timers = await WorkflowProcessTimer.SelectByProcessIdAsync(connection, processId)
                .ConfigureAwait(false);
            
            return timers.Select(t => new ProcessTimer(t.Id, t.Name, t.NextExecutionDateTime)).ToList();
        }

        public virtual async Task<List<IProcessInstanceTreeItem>> GetProcessInstanceTreeAsync(Guid rootProcessId)
        {
            ProcessLockGuard.Read(rootProcessId); // abp-tx
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            return await ProcessInstanceTree.GetProcessTreeItemsByRootProcessIdAsync(connection, rootProcessId).ConfigureAwait(false);
        }

        public virtual async Task<List<ProcessTimer>> GetActiveTimersForProcessAsync(Guid processId)
        {
            ProcessLockGuard.Read(processId); // abp-tx
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            
            IEnumerable<ProcessTimerEntity> timers = await WorkflowProcessTimer.SelectActiveByProcessIdAsync(connection, processId)
                .ConfigureAwait(false);
            
            return timers.Select(t => new ProcessTimer(t.Id, t.Name, t.NextExecutionDateTime)).ToList();
        }

        public virtual async Task<WorkflowRuntimeModel> GetWorkflowRuntimeModelAsync(string runtimeId)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            return await WorkflowRuntime.GetWorkflowRuntimeStatusAsync(connection, runtimeId).ConfigureAwait(false);
        }

        public virtual async Task<int> SendRuntimeLastAliveSignalAsync()
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            return await WorkflowRuntime.SendRuntimeLastAliveSignalAsync(connection, _runtime.Id, _runtime.RuntimeDateTimeNow)
                .ConfigureAwait(false);
        }

        public virtual async Task<DateTime?> GetNextTimerDateAsync(TimerCategory timerCategory, int timerInterval)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            
            string categoryName = timerCategory.ToString();
            var syncLock = await WorkflowSync.GetByNameAsync(connection, categoryName).ConfigureAwait(false);

            if (syncLock == null)
            {
                throw new Exception($"Sync lock {categoryName} not found");
            }

            string nextTimeColumnName = timerCategory switch
            {
                TimerCategory.Timer => "NextTimerTime",
                TimerCategory.ServiceTimer => "NextServiceTimerTime",
                _ => throw new Exception($"Unknown sync lock name: {categoryName}")
            };

            DateTime? max = await WorkflowRuntime.GetMaxNextTimeAsync(connection, _runtime.Id, nextTimeColumnName).ConfigureAwait(false);

            DateTime result = _runtime.RuntimeDateTimeNow;

            if (max > result)
            {
                result = max.Value;
            }
                
            result += TimeSpan.FromMilliseconds(timerInterval);
                
            var newLock = Guid.NewGuid();
            
            using var localTx = LocalSqlTransaction.Begin(connection); var transaction = localTx.Transaction; // abp-tx
            
            await WorkflowRuntime.UpdateNextTimeAsync(connection, _runtime.Id, nextTimeColumnName, result, transaction)
                .ConfigureAwait(false);
                   
            int rowCount = await WorkflowSync.UpdateLockAsync(connection, categoryName, newLock, syncLock.Lock, transaction)
                .ConfigureAwait(false);

            if (rowCount == 0)
            {
                localTx.Rollback(); // abp-tx
                return null;
            }
                    
            localTx.Commit(); // abp-tx

            return result;
        }

        public virtual async Task<List<WorkflowRuntimeModel>> GetWorkflowRuntimesAsync()
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            return (await WorkflowRuntime.SelectAllAsync(connection).ConfigureAwait(false)).Select(GetModel).ToList();
        }

        ///<inheritdoc/>
        public void ConfigureMigrations(IMigrationRunnerBuilder builder, Assembly assembly)
        {
            builder.AddSqlServer();
            builder.WithGlobalConnectionString(ConnectionString);
            Assembly scanAssembly = assembly ?? typeof(MSSQLProvider).Assembly;
            builder.ScanIn(scanAssembly);
        }

        private WorkflowRuntimeModel GetModel(RuntimeEntity result)
        {
            return new WorkflowRuntimeModel
            {
                Lock = result.Lock,
                RuntimeId = result.RuntimeId,
                Status = result.Status,
                RestorerId = result.RestorerId,
                LastAliveSignal = result.LastAliveSignal,
                NextTimerTime = result.NextTimerTime
            };
        }

        #endregion

        #region ISchemePersistenceProvider

        public virtual async Task<SchemeDefinition<XElement>> GetProcessSchemeByProcessIdAsync(Guid processId)
        {
            ProcessLockGuard.Read(processId); // abp-tx
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            
            var processInstance = await WorkflowProcessInstance.SelectByKeyAsync(connection, processId).ConfigureAwait(false);
            
            if (processInstance == null)
            {
                throw new ProcessNotFoundException(processId);
            }

            if (!processInstance.SchemeId.HasValue)
            {
                throw SchemeNotFoundException.Create(processId, SchemeLocation.WorkflowProcessInstance);
            }

            SchemeDefinition<XElement> schemeDefinition = await GetProcessSchemeBySchemeIdAsync(processInstance.SchemeId.Value)
                .ConfigureAwait(false);
            
            schemeDefinition.IsDeterminingParametersChanged = processInstance.IsDeterminingParametersChanged;
            return schemeDefinition;
        }

        public virtual async Task<SchemeDefinition<XElement>> GetProcessSchemeBySchemeIdAsync(Guid schemeId)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            
            var processScheme = await WorkflowProcessScheme.SelectByKeyAsync(connection, schemeId).ConfigureAwait(false);

            if (processScheme == null || String.IsNullOrEmpty(processScheme.Scheme))
            {
                throw SchemeNotFoundException.Create(schemeId, SchemeLocation.WorkflowProcessScheme);
            }

            return ConvertToSchemeDefinition(processScheme);
        }

        public virtual async Task<SchemeDefinition<XElement>> GetProcessSchemeWithParametersAsync(string schemeCode, string definingParameters,
            Guid? rootSchemeId, bool ignoreObsolete)
        {
            IEnumerable<ProcessSchemeEntity> processSchemes;
            string hash = HashHelper.GenerateStringHash(definingParameters);
            bool? isObsolete = ignoreObsolete ? false : (bool?)null;
            
            using (var connectionLease = LeaseConnection())
            {
                var connection = connectionLease.Connection; // abp-tx
                processSchemes = await WorkflowProcessScheme.SelectAsync(connection,
                        schemeCode,
                        hash,
                        isObsolete,
                        rootSchemeId)
                    .ConfigureAwait(false);
            }

            if (!processSchemes.Any())
            {
                throw SchemeNotFoundException.Create(schemeCode, SchemeLocation.WorkflowProcessScheme, definingParameters);
            }

            if (processSchemes.Count() == 1)
            {
                var scheme = processSchemes.First();
                return ConvertToSchemeDefinition(scheme);
            }

            foreach (var processScheme in processSchemes.Where(processScheme => processScheme.DefiningParameters == definingParameters))
            {
                return ConvertToSchemeDefinition(processScheme);
            }

            throw SchemeNotFoundException.Create(schemeCode, SchemeLocation.WorkflowProcessScheme, definingParameters);
        }

        public virtual async Task SetSchemeIsObsoleteAsync(string schemeCode, IDictionary<string, object> parameters)
        {
            string definingParameters = DefiningParametersSerializer.Serialize(parameters);
            string definingParametersHash = HashHelper.GenerateStringHash(definingParameters);
            
            using var connectionLease = LeaseOwnConnection(); var connection = connectionLease.Connection; // abp-tx: 流程方案不随业务事务回滚
            await WorkflowProcessScheme.SetObsoleteAsync(connection, schemeCode, definingParametersHash).ConfigureAwait(false);
        }

        public virtual async Task SetSchemeIsObsoleteAsync(string schemeCode)
        {
            using var connectionLease = LeaseOwnConnection(); var connection = connectionLease.Connection; // abp-tx: 流程方案不随业务事务回滚
            await WorkflowProcessScheme.SetObsoleteAsync(connection, schemeCode).ConfigureAwait(false);
        }
        
        public virtual async Task DropUnusedWorkflowProcessSchemeAsync()
        {
            using var connectionLease = LeaseOwnConnection(); var connection = connectionLease.Connection; // abp-tx: 流程方案不随业务事务回滚
            await WorkflowProcessScheme.DeleteUnusedAsync(connection).ConfigureAwait(false);
        }
        
        public virtual async Task<SchemeDefinition<XElement>> SaveSchemeAsync(SchemeDefinition<XElement> scheme)
        {
            string definingParameters = scheme.DefiningParameters;
            string definingParametersHash = HashHelper.GenerateStringHash(definingParameters);

            using var connectionLease = LeaseOwnConnection(); var connection = connectionLease.Connection; // abp-tx: 流程方案不随业务事务回滚
            
            var oldSchemes = await WorkflowProcessScheme.SelectAsync(connection, scheme.SchemeCode, definingParametersHash,
                scheme.IsObsolete, scheme.RootSchemeId).ConfigureAwait(false);

            if (oldSchemes.Any())
            {
                var existing = oldSchemes.FirstOrDefault(oldScheme => oldScheme.DefiningParameters == definingParameters);
                if (existing != null)
                {
                    return ConvertToSchemeDefinition(existing);
                }
            }

            var newProcessScheme = new ProcessSchemeEntity
            {
                Id = scheme.Id,
                DefiningParameters = definingParameters,
                DefiningParametersHash = definingParametersHash,
                Scheme = scheme.Scheme.ToString(),
                SchemeCode = scheme.SchemeCode,
                RootSchemeCode = scheme.RootSchemeCode,
                RootSchemeId = scheme.RootSchemeId,
                AllowedActivities = JsonConvert.SerializeObject(scheme.AllowedActivities),
                StartingTransition = scheme.StartingTransition,
                IsObsolete = scheme.IsObsolete
            };

            await WorkflowProcessScheme.InsertAsync(connection, newProcessScheme).ConfigureAwait(false);

            return ConvertToSchemeDefinition(newProcessScheme);
        }

        public virtual async Task UpsertSchemeAsync(SchemeDefinition<XElement> scheme)
        {
            string definingParameters = scheme.DefiningParameters;
            string definingParametersHash = HashHelper.GenerateStringHash(definingParameters);
            
            var newProcessScheme = new ProcessSchemeEntity
            {
                Id = scheme.Id,
                DefiningParameters = definingParameters,
                DefiningParametersHash = definingParametersHash,
                Scheme = scheme.Scheme.ToString(),
                SchemeCode = scheme.SchemeCode,
                RootSchemeCode = scheme.RootSchemeCode,
                RootSchemeId = scheme.RootSchemeId,
                AllowedActivities = JsonConvert.SerializeObject(scheme.AllowedActivities),
                StartingTransition = scheme.StartingTransition,
                IsObsolete = scheme.IsObsolete
            };
            
            using var connectionLease = LeaseOwnConnection(); var connection = connectionLease.Connection; // abp-tx: 流程方案不随业务事务回滚
            
            await WorkflowProcessScheme.UpsertAsync(connection, newProcessScheme).ConfigureAwait(false);
        }

        public virtual async Task SaveSchemeAsync(string schemaCode, bool canBeInlined, List<string> inlinedSchemes, string scheme,
            List<string> tags)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            
            var newScheme = new SchemeEntity
            {
                Code = schemaCode,
                Scheme = scheme,
                CanBeInlined = canBeInlined,
                InlinedSchemes = inlinedSchemes.Any()
                    ? JsonConvert.SerializeObject(inlinedSchemes)
                    : null,
                Tags = TagHelper.ToTagStringForDatabase(tags)
            };

            await WorkflowScheme.UpsertAsync(connection, newScheme).ConfigureAwait(false);
        }

        public virtual async Task<XElement> GetSchemeAsync(string code)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            
            var scheme = await WorkflowScheme.SelectByKeyAsync(connection, code).ConfigureAwait(false);
            
            if (scheme == null || String.IsNullOrEmpty(scheme.Scheme))
            {
                throw SchemeNotFoundException.Create(code, SchemeLocation.WorkflowProcessScheme);
            }

            return XElement.Parse(scheme.Scheme);
        }

        public virtual async Task<List<string>> GetInlinedSchemeCodesAsync()
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            return await WorkflowScheme.GetInlinedSchemeCodesAsync(connection).ConfigureAwait(false);
        }

        public virtual async Task<List<string>> GetRelatedByInliningSchemeCodesAsync(string schemeCode)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            return await WorkflowScheme.GetRelatedSchemeCodesAsync(connection,schemeCode).ConfigureAwait(false);
        }

        public virtual async Task<List<string>> SearchSchemesByTagsAsync(params string[] tags)
        {
            return await SearchSchemesByTagsAsync(tags?.AsEnumerable()).ConfigureAwait(false);
        }

        public virtual async Task<List<string>> SearchSchemesByTagsAsync(IEnumerable<string> tags)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            return await WorkflowScheme.GetSchemeCodesByTagsAsync(connection, tags).ConfigureAwait(false);
        }

        public virtual async Task AddSchemeTagsAsync(string schemeCode, params string[] tags)
        {
            await AddSchemeTagsAsync(schemeCode, tags?.AsEnumerable()).ConfigureAwait(false);
        }

        public virtual async Task AddSchemeTagsAsync(string schemeCode, IEnumerable<string> tags)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            await WorkflowScheme.AddSchemeTagsAsync(connection, schemeCode, tags, _runtime.Builder).ConfigureAwait(false);
        }

        public virtual async Task RemoveSchemeTagsAsync(string schemeCode, params string[] tags)
        {
            await RemoveSchemeTagsAsync(schemeCode, tags?.AsEnumerable()).ConfigureAwait(false);
        }

        public virtual async Task RemoveSchemeTagsAsync(string schemeCode, IEnumerable<string> tags)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            await WorkflowScheme.RemoveSchemeTagsAsync(connection, schemeCode, tags, _runtime.Builder).ConfigureAwait(false);
        }

        public virtual async Task SetSchemeTagsAsync(string schemeCode, params string[] tags)
        {
            await SetSchemeTagsAsync(schemeCode, tags?.AsEnumerable()).ConfigureAwait(false);
        }

        public virtual async Task SetSchemeTagsAsync(string schemeCode, IEnumerable<string> tags)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            await WorkflowScheme.SetSchemeTagsAsync(connection, schemeCode, tags, _runtime.Builder).ConfigureAwait(false);
        }

        #endregion

        #region IWorkflowGenerator

        public virtual async Task<XElement> GenerateAsync(string schemeCode, Guid schemeId, IDictionary<string, object> parameters)
        {
            if (parameters.Count > 0)
            {
                throw new InvalidOperationException("Parameters not supported");
            }

            return await GetSchemeAsync(schemeCode).ConfigureAwait(false);
        }

        #endregion

        private SchemeDefinition<XElement> ConvertToSchemeDefinition(ProcessSchemeEntity workflowProcessScheme)
        {
            return new SchemeDefinition<XElement>(
                workflowProcessScheme.Id,
                workflowProcessScheme.RootSchemeId,
                workflowProcessScheme.SchemeCode,
                workflowProcessScheme.RootSchemeCode,
                XElement.Parse(workflowProcessScheme.Scheme),
                workflowProcessScheme.IsObsolete,
                false,
                JsonConvert.DeserializeObject<List<string>>(workflowProcessScheme.AllowedActivities ?? "null"),
                workflowProcessScheme.StartingTransition,
                workflowProcessScheme.DefiningParameters);
        }

        private async Task<Tuple<int,WorkflowRuntimeModel>> UpdateWorkflowRuntimeAsync(WorkflowRuntimeModel runtime, Action<WorkflowRuntimeModel> setter, 
            Func<SqlConnection, WorkflowRuntimeModel, Guid, Task<int>> updateMethod)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            
            Guid oldLock = runtime.Lock;
            
            setter(runtime);
            
            runtime.Lock = Guid.NewGuid();
            
            int cnt = await updateMethod(connection, runtime, oldLock).ConfigureAwait(false);
                
            if (cnt != 1)
            {
                return new Tuple<int, WorkflowRuntimeModel>(cnt,null);
            }
                
            return new Tuple<int, WorkflowRuntimeModel>(cnt,runtime);
        }

        #region Bulk methods

        public bool IsBulkOperationsSupported => true;

        public virtual async Task BulkInitProcessesAsync(List<ProcessInstance> instances, ProcessStatus status, CancellationToken token)
        {
            foreach (var pi in instances) ProcessLockGuard.Write(pi.ProcessId); // abp-tx
            await BulkInitProcessesAsync(instances, null, status, token).ConfigureAwait(false);
        }

        public virtual async Task BulkInitProcessesAsync(List<ProcessInstance> instances, List<TimerToRegister> timers, ProcessStatus status, CancellationToken token)
        {
            foreach (var pi in instances) ProcessLockGuard.Write(pi.ProcessId); // abp-tx
            if (token.IsCancellationRequested)
            {
                return;
            }

            bool needRegisterTimers = timers != null && timers.Any();

            var piDataTable = WorkflowProcessInstance.ToDataTable();
            var psDataTable = WorkflowProcessInstanceStatus.ToDataTable();
            var ppDataTable = WorkflowProcessInstancePersistence.ToDataTable();
            var ptDataTable = WorkflowProcessTimer.ToDataTable();

            foreach (ProcessInstance processInstance in instances)
            {
                DataRow processRow = piDataTable.NewRow();
                processRow["Id"] = processInstance.ProcessId;
                processRow["SchemeId"] = processInstance.SchemeId;
                processRow["ActivityName"] = processInstance.ProcessScheme.InitialActivity.Name;
                processRow["StateName"] = processInstance.ProcessScheme.InitialActivity.State;
                processRow["RootProcessId"] = processInstance.RootProcessId;
                processRow["TenantId"] = processInstance.TenantId;
                processRow["CalendarName"] = processInstance.CalendarName;
                if (processInstance.ParentProcessId.HasValue)
                {
                    processRow["ParentProcessId"] = processInstance.ParentProcessId;
                }
                piDataTable.Rows.Add(processRow);
                DataRow statusRow = psDataTable.NewRow();
                statusRow["Id"] = processInstance.ProcessId;
                statusRow["Lock"] = Guid.NewGuid();
                statusRow["Status"] = status.Id;
                statusRow["RuntimeId"] = _runtime.Id;
                statusRow["SetTime"] = _runtime.RuntimeDateTimeNow;
                psDataTable.Rows.Add(statusRow);

                var parametersToPersist = processInstance.ProcessParameters.Where(p => p.Purpose == ParameterPurpose.Persistence && p.Value != null).ToList();

                foreach (ParameterDefinitionWithValue parameter in parametersToPersist)
                {
                    DataRow parameterRow = ppDataTable.NewRow();
                    parameterRow["Id"] = Guid.NewGuid();
                    parameterRow["ProcessId"] = processInstance.ProcessId;
                    parameterRow["ParameterName"] = parameter.Name;
                    parameterRow["Value"] = ParametersSerializer.Serialize(parameter.Value, parameter.Type);
                    ppDataTable.Rows.Add(parameterRow);
                }
            }

            if (needRegisterTimers)
            {
                foreach (TimerToRegister timer in timers)
                {
                    DataRow timerRow = ptDataTable.NewRow();
                    timerRow["Id"] = Guid.NewGuid();
                    timerRow["ProcessId"] = timer.ProcessId;
                    timerRow["RootProcessId"] = timer.ProcessId;
                    timerRow["Name"] = timer.Name;
                    timerRow["NextExecutionDateTime"] = timer.ExecutionDateTime;
                    timerRow["Ignore"] = false;
                    ptDataTable.Rows.Add(timerRow);
                }
            }

            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            // ReSharper disable once MethodSupportsCancellation
            if (connection.State != ConnectionState.Open) await connection.OpenAsync().ConfigureAwait(false); // abp-tx
            using var localTx = LocalSqlTransaction.Begin(connection); var transaction = localTx.Transaction; // abp-tx
            var bulk = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction) 
                {DestinationTableName = WorkflowProcessInstance.ObjectName};
            bulk.ColumnMappings.Add("Id", "Id");
            bulk.ColumnMappings.Add("SchemeId", "SchemeId");
            bulk.ColumnMappings.Add("ActivityName", "ActivityName");
            bulk.ColumnMappings.Add("StateName", "StateName");
            bulk.ColumnMappings.Add("RootProcessId", "RootProcessId");
            bulk.ColumnMappings.Add("ParentProcessId", "ParentProcessId");
            bulk.ColumnMappings.Add("TenantId", "TenantId");
            bulk.ColumnMappings.Add("CalendarName", "CalendarName");
            await bulk.WriteToServerAsync(piDataTable, token).ConfigureAwait(false);
            bulk.DestinationTableName = WorkflowProcessInstanceStatus.ObjectName;
            bulk.ColumnMappings.Clear();
            bulk.ColumnMappings.Add("Id", "Id");
            bulk.ColumnMappings.Add("Lock", "Lock");
            bulk.ColumnMappings.Add("Status", "Status");
            bulk.ColumnMappings.Add("RuntimeId", "RuntimeId");
            bulk.ColumnMappings.Add("SetTime", "SetTime");
            await bulk.WriteToServerAsync(psDataTable, token).ConfigureAwait(false);
            if (ppDataTable.Rows.Count > 0)
            {
                bulk.DestinationTableName = WorkflowProcessInstancePersistence.ObjectName;
                bulk.ColumnMappings.Clear();
                bulk.ColumnMappings.Add("Id", "Id");
                bulk.ColumnMappings.Add("ProcessId", "ProcessId");
                bulk.ColumnMappings.Add("ParameterName", "ParameterName");
                bulk.ColumnMappings.Add("Value", "Value");
                await bulk.WriteToServerAsync(ppDataTable, token).ConfigureAwait(false);
            }
            if (needRegisterTimers)
            {
                bulk.DestinationTableName = WorkflowProcessTimer.ObjectName;
                bulk.ColumnMappings.Clear();
                bulk.ColumnMappings.Add("Id", "Id");
                bulk.ColumnMappings.Add("ProcessId", "ProcessId");
                bulk.ColumnMappings.Add("RootProcessId", "RootProcessId");
                bulk.ColumnMappings.Add("Name", "Name");
                bulk.ColumnMappings.Add("NextExecutionDateTime", "NextExecutionDateTime");
                bulk.ColumnMappings.Add("Ignore", "Ignore");
                await bulk.WriteToServerAsync(ptDataTable, token).ConfigureAwait(false);
            }

            if (token.IsCancellationRequested)
            {
                localTx.Rollback(); // abp-tx
            }
            else
            {
                localTx.Commit(); // abp-tx
            }
        }

        #endregion

        #region IApprovalProvider

        public virtual async Task DropWorkflowInboxAsync(Guid processId)
        {
            ProcessLockGuard.Write(processId); // abp-tx
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            await WorkflowInbox.DeleteByAsync(connection, x => x.ProcessId, processId)
                .ConfigureAwait(false);
        }

        public virtual async Task InsertInboxAsync(List<InboxItem> newActors)
        {
            foreach (var item in newActors) ProcessLockGuard.Write(item.ProcessId); // abp-tx
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            InboxEntity[] inboxItems = newActors.Select(WorkflowInbox.ToDB).ToArray();
            await WorkflowInbox.InsertAllAsync(connection, inboxItems).ConfigureAwait(false);
        }

        public async Task<int> GetInboxCountByProcessIdAsync(Guid processId)
        {
            ProcessLockGuard.Read(processId); // abp-tx
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            return await WorkflowInbox.GetCountByAsync(connection, x => x.ProcessId, processId)
                .ConfigureAwait(false);
        }

        public async Task<int> GetInboxCountByIdentityIdAsync(string identityId)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            return await WorkflowInbox.GetCountByAsync(connection, x => x.IdentityId, identityId)
                .ConfigureAwait(false);
        }

        public virtual async Task<List<InboxItem>> GetInboxByProcessIdAsync(Guid processId, Paging paging=null, CultureInfo culture = null)
        {
            ProcessLockGuard.Read(processId); // abp-tx
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            
            InboxEntity[] inboxItems = await WorkflowInbox.SelectByWithPagingAsync(connection,
                    x => x.ProcessId, processId,
                    x => x.AddingDate, SortDirection.Desc,
                    paging)
                .ConfigureAwait(false);

            return await WorkflowInbox.FromDB(_runtime, inboxItems, culture ?? CultureInfo.CurrentCulture)
                .ConfigureAwait(false);
        }
       
        public virtual async Task<List<InboxItem>> GetInboxByIdentityIdAsync(string identityId, Paging paging=null, CultureInfo culture = null)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            
            InboxEntity[] inboxItems = await WorkflowInbox.SelectByWithPagingAsync(connection,
                    x => x.IdentityId, identityId,
                    x => x.AddingDate, SortDirection.Desc,
                    paging)
                .ConfigureAwait(false);
           
            return await WorkflowInbox.FromDB(_runtime, inboxItems, culture ?? CultureInfo.CurrentCulture)
                .ConfigureAwait(false);
        }
       
        public async Task FillApprovalHistoryAsync(ApprovalHistoryItem approvalHistoryItem)
        {
            ProcessLockGuard.Write(approvalHistoryItem.ProcessId); // abp-tx
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx

            var approvalHistory = (await WorkflowApprovalHistory.SelectByProcessIdAsync(connection, approvalHistoryItem.ProcessId)
                    .ConfigureAwait(false)).FirstOrDefault(h => !h.TransitionTime.HasValue 
                     && h.InitialState == approvalHistoryItem.InitialState 
                     && h.DestinationState == approvalHistoryItem.DestinationState);

            if (approvalHistory is null)
            {
                approvalHistory =  WorkflowApprovalHistory.ToDB(approvalHistoryItem);

                await WorkflowApprovalHistory.InsertAsync(connection, approvalHistory).ConfigureAwait(false);
            }
            else
            {
                approvalHistory.TriggerName = approvalHistoryItem.TriggerName;
                approvalHistory.TransitionTime = approvalHistoryItem.TransitionTime;
                approvalHistory.IdentityId = approvalHistoryItem.IdentityId;
                approvalHistory.Commentary = approvalHistoryItem.Commentary;
               
                await WorkflowApprovalHistory.UpdateAsync(connection, approvalHistory).ConfigureAwait(false);
            }
        }

        public async Task DropApprovalHistoryByIdentityIdAsync(string identityId)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            await WorkflowApprovalHistory.DeleteByAsync(connection, x => x.IdentityId, identityId).ConfigureAwait(false);
        }

        public virtual async Task DropEmptyApprovalHistoryAsync(Guid processId)
        {
            ProcessLockGuard.Write(processId); // abp-tx
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            
            foreach (var record in (await WorkflowApprovalHistory.SelectByProcessIdAsync(connection, processId)
                .ConfigureAwait(false)).Where(x => !x.TransitionTime.HasValue).ToList())
            {
                await WorkflowApprovalHistory.DeleteAsync(connection, record.Id).ConfigureAwait(false);
            }
        }

        public async Task DropApprovalHistoryByProcessIdAsync(Guid processId)
        {
            ProcessLockGuard.Write(processId); // abp-tx
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            await WorkflowApprovalHistory.DeleteByAsync(connection, x => x.ProcessId, processId)
                .ConfigureAwait(false);
        }
       
        public async Task<int> GetApprovalHistoryCountByProcessIdAsync(Guid processId)
        {
            ProcessLockGuard.Read(processId); // abp-tx
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            return await WorkflowApprovalHistory.GetCountByAsync(connection, x => x.ProcessId, processId)
                .ConfigureAwait(false);
        }

        public async Task<int> GetApprovalHistoryCountByIdentityIdAsync(string identityId)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            return await WorkflowApprovalHistory.GetCountByAsync(connection, x => x.IdentityId, identityId)
                .ConfigureAwait(false);
        }

        public virtual async  Task<List<ApprovalHistoryItem>> GetApprovalHistoryByProcessIdAsync(Guid processId, Paging paging=null)
        {
            ProcessLockGuard.Read(processId); // abp-tx
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            
            return (await WorkflowApprovalHistory.SelectByWithPagingAsync(connection,
                    x => x.ProcessId, processId,
                    x => x.Sort, SortDirection.Asc,
                    paging)
                .ConfigureAwait(false)).Select(WorkflowApprovalHistory.FromDB).ToList();
        }

        public virtual async  Task<List<ApprovalHistoryItem>> GetApprovalHistoryByIdentityIdAsync(string identityId, Paging paging=null)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            
            return (await WorkflowApprovalHistory.SelectByWithPagingAsync(connection,
                    x => x.IdentityId, identityId,
                    x => x.Sort, SortDirection.Asc,
                    paging)
                .ConfigureAwait(false)).Select(WorkflowApprovalHistory.FromDB).ToList();
        }

        public async Task<int> GetOutboxCountByIdentityIdAsync(string identityId)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            return await WorkflowApprovalHistory.GetOutboxCountByIdentityIdAsync(connection, identityId)
                .ConfigureAwait(false);
        }
       
        public virtual async Task<List<OutboxItem>> GetOutboxByIdentityIdAsync(string identityId, Paging paging = null)
        {
            using var connectionLease = LeaseConnection(); var connection = connectionLease.Connection; // abp-tx
            return await WorkflowApprovalHistory.SelectOutboxByIdentityIdAsync(connection, identityId, paging)
                .ConfigureAwait(false);
        }

        #endregion IApprovalProvider
    }
}
