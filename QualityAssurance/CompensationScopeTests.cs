using System;
using System.Configuration;
using System.Linq;
using AzureCrossTableConsistencySample;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace QualityAssurance
{
    [TestClass]
    public class CompensationScopeTests
    {
        private const int NumberOfOperations = 32;
        private CloudTable cloudTable;

        [TestInitialize]
        public void Initialize()
        {
            // Initialize the table client.
            var testTableName = String.Format("compensationscopetest{0}", DateTime.Now.Ticks);
            var storageAccount = CloudStorageAccount.Parse(ConfigurationManager.AppSettings["StorageConnectionString"]); ;
            var tableStorage = storageAccount.CreateCloudTableClient();
            
            cloudTable = tableStorage.GetTableReference(testTableName);
            cloudTable.CreateIfNotExists();
        }

        [TestMethod]
        public void CanInsertEntity()
        {
            var sampleEntity = CreateSampleEntity();

            using (var scope = new CompensationScope())
            {
                var insertOperation = TableOperation.Insert(sampleEntity);

                cloudTable.Execute(scope, insertOperation);

                // Indicate that all operations within the scope are completed successfully.
                scope.Complete();
            }

            var tableQuery = cloudTable.CreateQuery<SampleEntity>();
            var tableEntities = tableQuery.ToList();
            var firstEntity = tableEntities.FirstOrDefault();

            Assert.AreEqual(1, tableEntities.Count, "The table does not hold any data");
            Assert.IsNotNull(firstEntity, "The table does not have any entities");
            Assert.AreEqual(sampleEntity.PartitionKey, firstEntity.PartitionKey);
            Assert.AreEqual(sampleEntity.RowKey, firstEntity.RowKey);
        }

        [TestMethod]
        public void CanUndoInsertOperation()
        {
            var sampleEntity = CreateSampleEntity();

            using (var scope = new CompensationScope())
            {
                var insertOperation = TableOperation.Insert(sampleEntity);

                cloudTable.Execute(scope, insertOperation);

                // DO NOT call scope.Complete() to request a rollback.
            }

            var tableQuery = cloudTable.CreateQuery<SampleEntity>();
            var tableEntities = tableQuery.ToList();

            Assert.AreEqual(0, tableEntities.Count, "The table must not hold any data");
        }

        [TestMethod]
        public void CanDeleteEntity()
        {
            var sampleEntity = CreateSampleEntity();
            var insertOperation = TableOperation.Insert(sampleEntity);

            // Insert the sample entity
            cloudTable.Execute(insertOperation);

            // Read the sample entity back.
            var tableQuery = cloudTable.CreateQuery<SampleEntity>();
            var tableEntities = tableQuery.ToList();
            var firstEntity = tableEntities.FirstOrDefault();

            // Validate the sample entity.
            Assert.AreEqual(1, tableEntities.Count, "The table does not hold any data");
            Assert.IsNotNull(firstEntity, "The table does not have any entities");
            Assert.AreEqual(sampleEntity.PartitionKey, firstEntity.PartitionKey);
            Assert.AreEqual(sampleEntity.RowKey, firstEntity.RowKey);

            using (var scope = new CompensationScope())
            {
                var deleteOperation = TableOperation.Delete(sampleEntity);

                // Delete the sample entity
                cloudTable.Execute(scope, deleteOperation);

                // Indicate that all operations within the scope are completed successfully.
                scope.Complete();
            }

            tableEntities = tableQuery.ToList();
            Assert.AreEqual(0, tableEntities.Count, "The table must not hold any data");
        }

        [TestMethod]
        public void CanUndoDeleteOperation()
        {
            var sampleEntity = CreateSampleEntity();
            var insertOperation = TableOperation.Insert(sampleEntity);

            // Insert the sample entity
            cloudTable.Execute(insertOperation);

            // Read the sample entity back.
            var tableQuery = cloudTable.CreateQuery<SampleEntity>();
            var tableEntities = tableQuery.ToList();
            var firstEntity = tableEntities.FirstOrDefault();

            // Validate the sample entity.
            Assert.AreEqual(1, tableEntities.Count, "The table does not hold any data");
            Assert.IsNotNull(firstEntity, "The table does not have any entities");
            Assert.AreEqual(sampleEntity.PartitionKey, firstEntity.PartitionKey);
            Assert.AreEqual(sampleEntity.RowKey, firstEntity.RowKey);

            using (var scope = new CompensationScope())
            {
                var deleteOperation = TableOperation.Delete(sampleEntity);

                // Delete the sample entity
                cloudTable.Execute(scope, deleteOperation);

                // DO NOT call scope.Complete() to request a rollback.
            }

            tableEntities = tableQuery.ToList();
            firstEntity = tableEntities.FirstOrDefault();

            // Validate the sample entity.
            Assert.AreEqual(1, tableEntities.Count, "The table does not hold any data");
            Assert.IsNotNull(firstEntity, "The table does not have any entities");
            Assert.AreEqual(sampleEntity.PartitionKey, firstEntity.PartitionKey);
            Assert.AreEqual(sampleEntity.RowKey, firstEntity.RowKey);
            Assert.AreEqual(sampleEntity.EntryDate, firstEntity.EntryDate);
            Assert.AreEqual(sampleEntity.Client, firstEntity.Client);
            Assert.AreEqual(sampleEntity.ErrorID, firstEntity.ErrorID);
        }


        [TestMethod]
        public void CanInsertOrMergeEntity()
        {
            var sampleEntity = CreateSampleEntity();

            using (var scope = new CompensationScope())
            {
                var insertOperation = TableOperation.InsertOrMerge(sampleEntity);

                // Insert the sample entity.
                cloudTable.Execute(scope, insertOperation);

                // Read the sample entity back.
                var tableQuery = cloudTable.CreateQuery<SampleEntity>();
                var tableEntities = tableQuery.ToList();
                var firstEntity = tableEntities.FirstOrDefault();

                // Validate the sample entity.
                Assert.AreEqual(1, tableEntities.Count, "The table does not hold any data");
                Assert.IsNotNull(firstEntity, "The table does not have any entities");
                Assert.AreEqual(sampleEntity.PartitionKey, firstEntity.PartitionKey);
                Assert.AreEqual(sampleEntity.RowKey, firstEntity.RowKey);

                // Update sample entity with new data before merging.
                sampleEntity.EntryDate = sampleEntity.EntryDate.AddDays(100);
                sampleEntity.Client = String.Format("Updated Client:{0}", DateTime.Now.Ticks);
                sampleEntity.ErrorID = Guid.NewGuid();
                sampleEntity.ErrorMessage = null;

                var mergeOperation = TableOperation.InsertOrMerge(sampleEntity);

                // Merge the sample entity.
                cloudTable.Execute(scope, mergeOperation);

                // Indicate that all operations within the scope are completed successfully.
                scope.Complete();
            }

            var newTableQuery = cloudTable.CreateQuery<SampleEntity>();
            var allTableEntities = newTableQuery.ToList();
            var veryFirstEntity = allTableEntities.FirstOrDefault();

            // Validate the sample entity.
            Assert.AreEqual(1, allTableEntities.Count, "The table does not hold any data");
            Assert.IsNotNull(veryFirstEntity, "The table does not have any entities");
            Assert.AreEqual(sampleEntity.PartitionKey, veryFirstEntity.PartitionKey);
            Assert.AreEqual(sampleEntity.RowKey, veryFirstEntity.RowKey);
            Assert.AreEqual(sampleEntity.EntryDate, veryFirstEntity.EntryDate);
            Assert.AreEqual(sampleEntity.Client, veryFirstEntity.Client);
            Assert.AreEqual(sampleEntity.ErrorID, veryFirstEntity.ErrorID);
            Assert.IsNotNull(veryFirstEntity.ErrorMessage);
        }

        [TestMethod]
        public void CanUndoInsertOrMergeOperation()
        {
            var sampleEntity = CreateSampleEntity();

            using (var scope = new CompensationScope())
            {
                var insertOperation = TableOperation.InsertOrMerge(sampleEntity);

                // Insert the sample entity.
                cloudTable.Execute(scope, insertOperation);

                // DO NOT call scope.Complete() to request a rollback.
            }

            var tableQuery = cloudTable.CreateQuery<SampleEntity>();
            var tableEntities = tableQuery.ToList();

            Assert.AreEqual(0, tableEntities.Count, "The table must not hold any data");

            var reinsertOperation = TableOperation.InsertOrMerge(sampleEntity);

            // Re-insert the sample entity.
            cloudTable.Execute(reinsertOperation);

            // Update sample entity with new data before merging.
            sampleEntity.EntryDate = sampleEntity.EntryDate.AddDays(100);
            sampleEntity.Client = String.Format("Updated Client:{0}", DateTime.Now.Ticks);
            sampleEntity.ErrorID = Guid.NewGuid();

            using (var scope = new CompensationScope())
            {
                var mergeOperation = TableOperation.InsertOrMerge(sampleEntity);

                // Merge the sample entity.
                cloudTable.Execute(scope, mergeOperation);

                // DO NOT call scope.Complete() to request a rollback.
            }

            var newTableQuery = cloudTable.CreateQuery<SampleEntity>();
            var allTableEntities = newTableQuery.ToList();
            var veryFirstEntity = allTableEntities.FirstOrDefault();

            // Validate the sample entity.
            Assert.AreEqual(1, allTableEntities.Count, "The table does not hold any data");
            Assert.IsNotNull(veryFirstEntity, "The table does not have any entities");
            Assert.AreEqual(sampleEntity.PartitionKey, veryFirstEntity.PartitionKey);
            Assert.AreEqual(sampleEntity.RowKey, veryFirstEntity.RowKey);
            Assert.AreNotEqual(sampleEntity.EntryDate, veryFirstEntity.EntryDate);
            Assert.AreNotEqual(sampleEntity.Client, veryFirstEntity.Client);
            Assert.AreNotEqual(sampleEntity.ErrorID, veryFirstEntity.ErrorID);
        }

        [TestMethod]
        public void CanInsertOrReplaceEntity()
        {
            var sampleEntity = CreateSampleEntity();

            using (var scope = new CompensationScope())
            {
                var insertOperation = TableOperation.InsertOrReplace(sampleEntity);

                // Insert the sample entity.
                cloudTable.Execute(scope, insertOperation);

                // Read the sample entity back.
                var tableQuery = cloudTable.CreateQuery<SampleEntity>();
                var tableEntities = tableQuery.ToList();
                var firstEntity = tableEntities.FirstOrDefault();

                // Validate the sample entity.
                Assert.AreEqual(1, tableEntities.Count, "The table does not hold any data");
                Assert.IsNotNull(firstEntity, "The table does not have any entities");
                Assert.AreEqual(sampleEntity.PartitionKey, firstEntity.PartitionKey);
                Assert.AreEqual(sampleEntity.RowKey, firstEntity.RowKey);

                // Update sample entity with new data before merging.
                sampleEntity.EntryDate = sampleEntity.EntryDate.AddDays(100);
                sampleEntity.Client = String.Format("Updated Client:{0}", DateTime.Now.Ticks);
                sampleEntity.ErrorID = Guid.NewGuid();
                sampleEntity.ErrorMessage = null;

                var replaceOperation = TableOperation.InsertOrReplace(sampleEntity);

                // Merge the sample entity.
                cloudTable.Execute(scope, replaceOperation);

                // Indicate that all operations within the scope are completed successfully.
                scope.Complete();
            }

            var newTableQuery = cloudTable.CreateQuery<SampleEntity>();
            var allTableEntities = newTableQuery.ToList();
            var veryFirstEntity = allTableEntities.FirstOrDefault();

            // Validate the sample entity.
            Assert.AreEqual(1, allTableEntities.Count, "The table does not hold any data");
            Assert.IsNotNull(veryFirstEntity, "The table does not have any entities");
            Assert.AreEqual(sampleEntity.PartitionKey, veryFirstEntity.PartitionKey);
            Assert.AreEqual(sampleEntity.RowKey, veryFirstEntity.RowKey);
            Assert.AreEqual(sampleEntity.EntryDate, veryFirstEntity.EntryDate);
            Assert.AreEqual(sampleEntity.Client, veryFirstEntity.Client);
            Assert.AreEqual(sampleEntity.ErrorID, veryFirstEntity.ErrorID);
            Assert.IsNull(veryFirstEntity.ErrorMessage);
        }

        [TestMethod]
        public void CanUndoInsertOrReplaceOperation()
        {
            var sampleEntity = CreateSampleEntity();

            using (var scope = new CompensationScope())
            {
                var insertOperation = TableOperation.InsertOrReplace(sampleEntity);

                // Insert the sample entity.
                cloudTable.Execute(scope, insertOperation);

                // DO NOT call scope.Complete() to request a rollback.
            }

            var tableQuery = cloudTable.CreateQuery<SampleEntity>();
            var tableEntities = tableQuery.ToList();

            Assert.AreEqual(0, tableEntities.Count, "The table must not hold any data");

            var reinsertOperation = TableOperation.InsertOrReplace(sampleEntity);

            // Re-insert the sample entity.
            cloudTable.Execute(reinsertOperation);

            // Update sample entity with new data before merging.
            sampleEntity.EntryDate = sampleEntity.EntryDate.AddDays(100);
            sampleEntity.Client = String.Format("Updated Client:{0}", DateTime.Now.Ticks);
            sampleEntity.ErrorID = Guid.NewGuid();
            sampleEntity.ErrorMessage = null;

            using (var scope = new CompensationScope())
            {
                var replaceOperation = TableOperation.InsertOrReplace(sampleEntity);

                // Replace the sample entity.
                cloudTable.Execute(scope, replaceOperation);

                // DO NOT call scope.Complete() to request a rollback.
            }

            var newTableQuery = cloudTable.CreateQuery<SampleEntity>();
            var allTableEntities = newTableQuery.ToList();
            var veryFirstEntity = allTableEntities.FirstOrDefault();

            // Validate the sample entity.
            Assert.AreEqual(1, allTableEntities.Count, "The table does not hold any data");
            Assert.IsNotNull(veryFirstEntity, "The table does not have any entities");
            Assert.AreEqual(sampleEntity.PartitionKey, veryFirstEntity.PartitionKey);
            Assert.AreEqual(sampleEntity.RowKey, veryFirstEntity.RowKey);
            Assert.AreNotEqual(sampleEntity.EntryDate, veryFirstEntity.EntryDate);
            Assert.AreNotEqual(sampleEntity.Client, veryFirstEntity.Client);
            Assert.AreNotEqual(sampleEntity.ErrorID, veryFirstEntity.ErrorID);
            Assert.AreNotEqual(sampleEntity.ErrorMessage, veryFirstEntity.ErrorMessage);
        }

        [TestMethod]
        public void CanMergeEntity()
        {
            var sampleEntity = CreateSampleEntity();

            using (var scope = new CompensationScope())
            {
                var insertOperation = TableOperation.Insert(sampleEntity);

                // Insert the sample entity.
                cloudTable.Execute(scope, insertOperation);

                // Update sample entity with new data before merging.
                sampleEntity.EntryDate = sampleEntity.EntryDate.AddDays(100);
                sampleEntity.Client = String.Format("Updated Client:{0}", DateTime.Now.Ticks);
                sampleEntity.ErrorID = Guid.NewGuid();
                sampleEntity.ErrorMessage = null;

                var mergeOperation = TableOperation.Merge(sampleEntity);

                // Merge the sample entity.
                cloudTable.Execute(scope, mergeOperation);

                // Indicate that all operations within the scope are completed successfully.
                scope.Complete();
            }

            var newTableQuery = cloudTable.CreateQuery<SampleEntity>();
            var allTableEntities = newTableQuery.ToList();
            var veryFirstEntity = allTableEntities.FirstOrDefault();

            // Validate the sample entity.
            Assert.AreEqual(1, allTableEntities.Count, "The table does not hold any data");
            Assert.IsNotNull(veryFirstEntity, "The table does not have any entities");
            Assert.AreEqual(sampleEntity.PartitionKey, veryFirstEntity.PartitionKey);
            Assert.AreEqual(sampleEntity.RowKey, veryFirstEntity.RowKey);
            Assert.AreEqual(sampleEntity.EntryDate, veryFirstEntity.EntryDate);
            Assert.AreEqual(sampleEntity.Client, veryFirstEntity.Client);
            Assert.AreEqual(sampleEntity.ErrorID, veryFirstEntity.ErrorID);
            Assert.IsNotNull(veryFirstEntity.ErrorMessage);
        }

        [TestMethod]
        public void CanUndoMergeOperation()
        {
            var sampleEntity = CreateSampleEntity();
            var insertOperation = TableOperation.Insert(sampleEntity);

            // Insert the sample entity.
            cloudTable.Execute(insertOperation);

            // Remember the original values.
            var originalEntryDate = sampleEntity.EntryDate;
            var originalClient = sampleEntity.Client;
            var originalErrorId = sampleEntity.ErrorID;

            using (var scope = new CompensationScope())
            {
                // Update sample entity with new data before merging.
                sampleEntity.EntryDate = sampleEntity.EntryDate.AddDays(100);
                sampleEntity.Client = String.Format("Updated Client:{0}", DateTime.Now.Ticks);
                sampleEntity.ErrorID = Guid.NewGuid();

                var mergeOperation = TableOperation.Merge(sampleEntity);

                // Merge the sample entity.
                cloudTable.Execute(scope, mergeOperation);

                // DO NOT call scope.Complete() to request a rollback.
            }

            var newTableQuery = cloudTable.CreateQuery<SampleEntity>();
            var allTableEntities = newTableQuery.ToList();
            var veryFirstEntity = allTableEntities.FirstOrDefault();

            // Validate the sample entity.
            Assert.AreEqual(1, allTableEntities.Count, "The table does not hold any data");
            Assert.IsNotNull(veryFirstEntity, "The table does not have any entities");
            Assert.AreEqual(sampleEntity.PartitionKey, veryFirstEntity.PartitionKey);
            Assert.AreEqual(sampleEntity.RowKey, veryFirstEntity.RowKey);
            Assert.AreEqual(originalEntryDate, veryFirstEntity.EntryDate);
            Assert.AreEqual(originalClient, veryFirstEntity.Client);
            Assert.AreEqual(originalErrorId, veryFirstEntity.ErrorID);
        }

        [TestMethod]
        public void CanReplaceEntity()
        {
            var sampleEntity = CreateSampleEntity();

            using (var scope = new CompensationScope())
            {
                var insertOperation = TableOperation.Insert(sampleEntity);

                // Insert the sample entity.
                cloudTable.Execute(scope, insertOperation);

                // Update sample entity with new data before merging.
                sampleEntity.EntryDate = sampleEntity.EntryDate.AddDays(100);
                sampleEntity.Client = String.Format("Updated Client:{0}", DateTime.Now.Ticks);
                sampleEntity.ErrorID = Guid.NewGuid();
                sampleEntity.ErrorMessage = null;

                var replaceOperation = TableOperation.Replace(sampleEntity);

                // Replace the sample entity.
                cloudTable.Execute(scope, replaceOperation);

                // Indicate that all operations within the scope are completed successfully.
                scope.Complete();
            }

            var newTableQuery = cloudTable.CreateQuery<SampleEntity>();
            var allTableEntities = newTableQuery.ToList();
            var veryFirstEntity = allTableEntities.FirstOrDefault();

            // Validate the sample entity.
            Assert.AreEqual(1, allTableEntities.Count, "The table does not hold any data");
            Assert.IsNotNull(veryFirstEntity, "The table does not have any entities");
            Assert.AreEqual(sampleEntity.PartitionKey, veryFirstEntity.PartitionKey);
            Assert.AreEqual(sampleEntity.RowKey, veryFirstEntity.RowKey);
            Assert.AreEqual(sampleEntity.EntryDate, veryFirstEntity.EntryDate);
            Assert.AreEqual(sampleEntity.Client, veryFirstEntity.Client);
            Assert.AreEqual(sampleEntity.ErrorID, veryFirstEntity.ErrorID);
            Assert.IsNull(veryFirstEntity.ErrorMessage);
        }

        [TestMethod]
        public void CanUndoReplaceOperation()
        {
            var sampleEntity = CreateSampleEntity();
            var insertOperation = TableOperation.Insert(sampleEntity);

            // Insert the sample entity.
            cloudTable.Execute(insertOperation);

            // Remember the original values.
            var originalEntryDate = sampleEntity.EntryDate;
            var originalClient = sampleEntity.Client;
            var originalErrorId = sampleEntity.ErrorID;

            using (var scope = new CompensationScope())
            {
                // Update sample entity with new data before merging.
                sampleEntity.EntryDate = sampleEntity.EntryDate.AddDays(100);
                sampleEntity.Client = String.Format("Updated Client:{0}", DateTime.Now.Ticks);
                sampleEntity.ErrorID = Guid.NewGuid();
                sampleEntity.ErrorMessage = null;

                var replaceOperation = TableOperation.Replace(sampleEntity);

                // Replace the sample entity.
                cloudTable.Execute(scope, replaceOperation);

                // DO NOT call scope.Complete() to request a rollback.
            }

            var newTableQuery = cloudTable.CreateQuery<SampleEntity>();
            var allTableEntities = newTableQuery.ToList();
            var veryFirstEntity = allTableEntities.FirstOrDefault();

            // Validate the sample entity.
            Assert.AreEqual(1, allTableEntities.Count, "The table does not hold any data");
            Assert.IsNotNull(veryFirstEntity, "The table does not have any entities");
            Assert.AreEqual(sampleEntity.PartitionKey, veryFirstEntity.PartitionKey);
            Assert.AreEqual(sampleEntity.RowKey, veryFirstEntity.RowKey);
            Assert.AreEqual(originalEntryDate, veryFirstEntity.EntryDate);
            Assert.AreEqual(originalClient, veryFirstEntity.Client);
            Assert.AreEqual(originalErrorId, veryFirstEntity.ErrorID);
            Assert.IsNotNull(veryFirstEntity.ErrorMessage);
        }

        [TestMethod]
        public void CanUndoMultipleInsertOperations()
        {
            using (var scope = new CompensationScope())
            {
                for (var i = 0; i < NumberOfOperations; i++)
                {
                    cloudTable.Execute(scope, TableOperation.Insert(CreateSampleEntity()));
                }

                // DO NOT call scope.Complete() to request a rollback.
            }

            var tableQuery = cloudTable.CreateQuery<SampleEntity>();
            var tableEntities = tableQuery.ToList();

            Assert.AreEqual(0, tableEntities.Count, "The table must not hold any data");
        }

        [TestMethod]
        public void CanUndoMultipleDeleteOperations()
        {
            using (var scope = new CompensationScope())
            {
                for (var i = 0; i < NumberOfOperations; i++)
                {
                    cloudTable.Execute(scope, TableOperation.Insert(CreateSampleEntity()));
                }

                scope.Complete();
            }

            var tableQuery = cloudTable.CreateQuery<SampleEntity>();
            var tableEntities = tableQuery.ToList();

            Assert.AreEqual(NumberOfOperations, tableEntities.Count, "The table does not hold any data after insert");

            using (var scope = new CompensationScope())
            {
                foreach(var entity in tableEntities)
                {
                    cloudTable.Execute(scope, TableOperation.Delete(entity));
                }

                // DO NOT call scope.Complete() to request a rollback.
            }

            var newTableQuery = cloudTable.CreateQuery<SampleEntity>();
            var newTableEntities = newTableQuery.ToList();

            Assert.AreEqual(NumberOfOperations, newTableEntities.Count, "The table does not hold any data after deletion");
        }

        [TestCleanup]
        public void Cleanup()
        {
            // Remove the table before each test so that we don't deal with stale data.
            cloudTable.DeleteIfExists();
        }

        private static SampleEntity CreateSampleEntity()
        {
            return new SampleEntity
            {
                PartitionKey = Guid.NewGuid().ToString("N"),
                RowKey = Guid.NewGuid().ToString("N"),
                EntryDate = DateTime.UtcNow,
                Client = "SampleClient",
                ErrorID = Guid.NewGuid(),
                ErrorMessage = "Sample error message"
            };
        }

        private class SampleEntity : TableEntity
        {
            public DateTime EntryDate { get; set; }
            public String Client { get; set; }
            public Guid ErrorID { get; set; }
            public String ErrorMessage { get; set; }
        }
    }
}

