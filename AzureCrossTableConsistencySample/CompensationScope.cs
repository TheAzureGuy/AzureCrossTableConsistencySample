using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using AzureCrossTableConsistencySample.Properties;
using Microsoft.Practices.TransientFaultHandling;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureCrossTableConsistencySample
{
    /// <summary>
    /// Represents a scoped context object that can be used by consuming application to track changes made in the Azure Table Storage and roll them back by the means of compensation.
    /// </summary>
    public sealed class CompensationScope : IDisposable
    {
        private static readonly Func<TableOperation, TableOperationType> GetOperationType = CreateExpressionTree<TableOperationType>("OperationType");
        private static readonly Func<TableOperation, ITableEntity> GetEntity = CreateExpressionTree<ITableEntity>("Entity");

        private readonly Queue<Action> compensators;
        private readonly TimeSpan timeout;
        private volatile bool disposed;
        private volatile bool complete;

        #region Constructors
        /// <summary>
        /// Initializes a new instance of the <see cref="CompensationScope"/> class with default settings.
        /// </summary>
        public CompensationScope()
            : this(TimeSpan.FromSeconds(120))
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CompensationScope"/> class using the specified <paramref name="timeout"/> which defines how long each undo action in the compensation scope is allowed to run.
        /// </summary>
        /// <param name="timeout">The amount of time defining how long each undo action in the compensation scope is allowed to run.</param>
        public CompensationScope(TimeSpan timeout)
        {
            // Initialize the list of undo actions.
            compensators = new Queue<Action>();
            this.timeout = timeout;
        }
        #endregion

        /// <summary>
        /// Executes an operation on a table that is governed by and tracked in this compensation scope.
        /// </summary>
        /// <param name="table">The Azure table where the specified <paramref name="operation"/> will be executed.</param>
        /// <param name="operation">An object that represents a single storage operation to be executed.</param>
        /// <param name="requestOptions">An object that specifies additional options for the request.</param>
        /// <param name="operationContext">An object that represents the context for the current operation.</param>
        /// <returns>The result of the operation performed in the Azure table.</returns>
        public TableResult Execute(CloudTable table, TableOperation operation, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            Guard.ArgumentNotNull(table, "table");
            Guard.ArgumentNotNull(operation, "operation");

            // Create the undo action for this operation.
            var undoAction = CreateUndoAction(table, operation, requestOptions, operationContext);

            // Execute the operation and allow it to successfully complete.
            var result = table.Execute(operation, requestOptions, operationContext);

            // Keep a track of the successful operation so that it can be undone if needed.
            if (undoAction != null) compensators.Enqueue(undoAction);

            return result;
        }

        /// <summary>
        /// Executes a batch operation on a table that is governed by and tracked in this compensation scope.
        /// </summary>
        /// <param name="table">The Azure table where the specified <paramref name="batch"/> of operations will be executed.</param>
        /// <param name="batch">An object that represents a batch of storage operations to be executed.</param>
        /// <param name="requestOptions">An object that specifies additional options for the request.</param>
        /// <param name="operationContext">An object that represents the context for the current operation.</param>
        /// <returns>The result of the operation performed in the Azure table.</returns>
        public IList<TableResult> Execute(CloudTable table, TableBatchOperation batch, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            Guard.ArgumentNotNull(table, "table");
            Guard.ArgumentNotNull(batch, "batch");

            // Create the undo action for each operation in the batch.
            var undoActions = new List<Action>(batch.Select(op => CreateUndoAction(table, op, requestOptions, operationContext)));

            // Execute the operation and allow it to successfully complete.
            var result = table.ExecuteBatch(batch, requestOptions, operationContext);

            // Keep a track of all successful operations so that these can be undone if needed.
            undoActions.ForEach(action => compensators.Enqueue(action));

            return result;
        }

        /// <summary>
        /// Indicates that all operations within the scope are completed successfully.
        /// </summary>
        public void Complete()
        {
            // Make sure the object is in the valid state.
            if (disposed) throw new ObjectDisposedException(typeof(CompensationScope).Name);
            if (complete) throw new InvalidOperationException(Resources.CompensationScopeAlreadyCompleted);

            complete = true;
        }

        #region IDisposable implementation
        /// <summary>
        /// Finalizes the object instance.
        /// </summary>
        ~CompensationScope()
        {
            Dispose(false);
        }

        /// <summary>
        /// Disposes of instance state.
        /// </summary>
        /// <param name="disposing">Determines whether this was called by Dispose or by the finalizer.</param>
        private void Dispose(bool disposing)
        {
            if (!disposed)
            {
                disposed = true;

                if (disposing)
                {
                    try
                    {
                        if (!complete) UndoAll();
                    }
                    finally
                    {
                        compensators.Clear();
                    }
                }
            }
        }

        /// <summary>
        /// Disposes of instance state.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        #endregion

        private Action CreateUndoAction(CloudTable table, TableOperation operation, TableRequestOptions requestOptions, OperationContext operationContext)
        {
            // Discover the operation type.
            var operationType = GetOperationType(operation);

            // Only those operations that modify the data are considered for compensation logic.
            if (operationType == TableOperationType.Retrieve) return null;

            // Find out what entity has been modified.
            var tableEntity = GetEntity(operation);

            // Create an undo function depending on the nature of operation performed.
            switch (operationType)
            {
                case TableOperationType.Insert:
                    return () => UndoInsertOperation(table, tableEntity, requestOptions, operationContext);
                case TableOperationType.Delete:
                    return () => UndoDeleteOperation(table, tableEntity, requestOptions, operationContext);
                case TableOperationType.InsertOrMerge:
                    // Obtain the original entity which helps determine whether Insert or Merge operation will actually be performed.
                    var retrieveResult = table.Execute(TableOperation.Retrieve(tableEntity.PartitionKey, tableEntity.RowKey));
                    return () => UndoInsertOrMergeOperation(table, retrieveResult.Result as DynamicTableEntity, tableEntity, requestOptions, operationContext);
                case TableOperationType.InsertOrReplace:
                    // Obtain the original entity which helps determine whether Insert or Replace operation will actually be performed.
                    retrieveResult = table.Execute(TableOperation.Retrieve(tableEntity.PartitionKey, tableEntity.RowKey));
                    return () => UndoInsertOrReplaceOperation(table, retrieveResult.Result as DynamicTableEntity, tableEntity, requestOptions, operationContext);
                case TableOperationType.Merge:
                    // Obtain the original entity which helps roll changes back.
                    retrieveResult = table.Execute(TableOperation.Retrieve(tableEntity.PartitionKey, tableEntity.RowKey));
                    return () => UndoMergeOperation(table, retrieveResult.Result as DynamicTableEntity, tableEntity, requestOptions, operationContext);
                case TableOperationType.Replace:
                    // Obtain the original entity which helps roll changes back.
                    retrieveResult = table.Execute(TableOperation.Retrieve(tableEntity.PartitionKey, tableEntity.RowKey));
                    return () => UndoReplaceOperation(table, retrieveResult.Result as DynamicTableEntity, tableEntity, requestOptions, operationContext);
                default:
                    throw new InvalidOperationException(String.Format(Resources.CompensationScopeInvalidOperation, operationType));
            }
        }

        private void UndoAll()
        {
            // Walk through the set of undo actions and invoke them in the reverse order.
            while (compensators.Count > 0)
            {
                // Get the next undo action;
                var undoAction = compensators.Dequeue();

                // Perform the undo operation.
                undoAction();
            }
        }

        private void UndoInsertOperation(CloudTable table, ITableEntity entity, TableRequestOptions requestOptions, OperationContext operationContext)
        {
            var deleteOperation = TableOperation.Delete(entity);

            // Execute the undo action that removes the entity that was previously inserted.
            table.Execute(deleteOperation, CreateUndoRequestOptions(requestOptions), operationContext);
        }

        private void UndoDeleteOperation(CloudTable table, ITableEntity entity, TableRequestOptions requestOptions, OperationContext operationContext)
        {
            var insertOperation = TableOperation.Insert(entity);

            // Execute the undo action that inserts the entity that was previously deleted.
            table.Execute(insertOperation, CreateUndoRequestOptions(requestOptions), operationContext);
        }

        private void UndoMergeOperation(CloudTable table, ITableEntity originalEntity, ITableEntity newEntity, TableRequestOptions requestOptions, OperationContext operationContext)
        {
            // Guard against entities that were deleted after performing a merge.
            if (originalEntity != null)
            {
                // Take the eTag value from the new entity (if available) that holds the current identifier associated with that entity in the storage service.
                if (!String.IsNullOrEmpty(newEntity.ETag)) originalEntity.ETag = newEntity.ETag;

                // Instantiate the Replace operation.
                var replaceOperation = TableOperation.Replace(originalEntity);

                // Execute the undo action that reverts changes in the entity that was previously merged.
                table.Execute(replaceOperation, CreateUndoRequestOptions(requestOptions), operationContext);
            }
        }

        private void UndoReplaceOperation(CloudTable table, ITableEntity originalEntity, ITableEntity newEntity, TableRequestOptions requestOptions, OperationContext operationContext)
        {
            // Guard against entities that were deleted after performing a merge.
            if (originalEntity != null)
            {
                // Take the eTag value from the new entity (if available) that holds the current identifier associated with that entity in the storage service.
                if (!String.IsNullOrEmpty(newEntity.ETag)) originalEntity.ETag = newEntity.ETag;

                // Instantiate the Replace operation.
                var replaceOperation = TableOperation.Replace(originalEntity);

                // Execute the undo action that reverts changes in the entity that was previously replaced.
                table.Execute(replaceOperation, CreateUndoRequestOptions(requestOptions), operationContext);
            }
        }

        private void UndoInsertOrMergeOperation(CloudTable table, ITableEntity originalEntity, ITableEntity newEntity, TableRequestOptions requestOptions, OperationContext operationContext)
        {
            // The presence of the original entity indicates that a Merge operation was performed.
            if (originalEntity != null)
            {
                // Perform an undo of the Merge operation.
                UndoMergeOperation(table, originalEntity, newEntity, requestOptions, operationContext);
            }
            else
            {
                // Perform an undo of the Insert operation.
                UndoInsertOperation(table, newEntity, requestOptions, operationContext);
            }
        }

        private void UndoInsertOrReplaceOperation(CloudTable table, ITableEntity originalEntity, ITableEntity newEntity, TableRequestOptions requestOptions, OperationContext operationContext)
        {
            // The presence of the original entity indicates that a Replace operation was performed.
            if (originalEntity != null)
            {
                // Perform an undo of the Replace operation.
                UndoReplaceOperation(table, originalEntity, newEntity, requestOptions, operationContext);
            }
            else
            {
                // Perform an undo of the Insert operation.
                UndoInsertOperation(table, newEntity, requestOptions, operationContext);
            }
        }

        private static Func<TableOperation, TResult> CreateExpressionTree<TResult>(string propertyName)
        {
            // Make sure the specified property exists, otherwise do not create the expression tree.
            var propertyInfo = typeof(TableOperation).GetProperty(propertyName, BindingFlags.IgnoreCase | BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.FlattenHierarchy);

            // Guard against renamed property (who knows).
            if (null == propertyInfo) throw new ArgumentNullException(propertyName);

            var entityParameter = Expression.Parameter(typeof(TableOperation), "op");
            var propertyExpr = Expression.Property(entityParameter, propertyName);

            return (Func<TableOperation, TResult>)Expression.Lambda(propertyExpr, entityParameter).Compile();
        }

        private TableRequestOptions CreateUndoRequestOptions(TableRequestOptions baseOptions)
        {
            // Create a custom TableRequestOptions instance which will be used for executing storage operations that undo changes.
            return new TableRequestOptions(baseOptions)
            {
                MaximumExecutionTime = timeout,
                RetryPolicy = new ExponentialRetry(TimeSpan.FromMilliseconds(100), Int32.MaxValue)
            };
        }
    }

    /// <summary>
    /// Provides a set of extension methods that supplement the <see cref="CompensationScope"/> class with value-add functionality and convenience.
    /// </summary>
    public static class CompensationScopeExtensions
    {
        /// <summary>
        /// Executes an operation on a table that is governed by and tracked in the specified compensation <paramref name="scope"/>.
        /// </summary>
        /// <param name="table">The Azure table where the specified <paramref name="operation"/> will be executed.</param>
        /// <param name="scope">The scope object tracking changes made in the Azure Table Storage.</param>
        /// <param name="operation">An object that represents a single storage operation to be executed.</param>
        /// <param name="requestOptions">An object that specifies additional options for the request.</param>
        /// <param name="operationContext">An object that represents the context for the current operation.</param>
        /// <returns>The result of the operation performed in the Azure table.</returns>
        public static TableResult Execute(this CloudTable table, CompensationScope scope, TableOperation operation, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            Guard.ArgumentNotNull(table, "table");
            Guard.ArgumentNotNull(scope, "scope");
            Guard.ArgumentNotNull(operation, "operation");

            return scope.Execute(table, operation, requestOptions, operationContext);
        }

        /// <summary>
        /// Executes a batch-aware operation on a table that is governed by and tracked in the specified compensation <paramref name="scope"/>.
        /// </summary>
        /// <param name="table">The Azure table where the specified <paramref name="operation"/> will be executed.</param>
        /// <param name="scope">The scope object tracking changes made in the Azure Table Storage.</param>
        /// <param name="batch">An object that represents a batch of storage operations to be executed.</param>
        /// <param name="requestOptions">An object that specifies additional options for the request.</param>
        /// <param name="operationContext">An object that represents the context for the current operation.</param>
        /// <returns>The result of the operation performed in the Azure table.</returns>
        public static IList<TableResult> Execute(this CloudTable table, CompensationScope scope, TableBatchOperation batch, TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            Guard.ArgumentNotNull(table, "table");
            Guard.ArgumentNotNull(scope, "scope");
            Guard.ArgumentNotNull(batch, "batch");

            return scope.Execute(table, batch, requestOptions, operationContext);
        }
    }
}
