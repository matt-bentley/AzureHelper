using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace AzureHelper
{
    public class TableHelper
    {
        private CloudStorageAccount _storageAccount;
        private CloudTableClient _tableClient;
        private CloudTable _table;

        public TableHelper(string tableName, string connectionString)
        {
            _storageAccount = CloudStorageAccount.Parse(connectionString);

            // Create the table client.
            _tableClient = _storageAccount.CreateCloudTableClient();

            // Retrieve a reference to the table.
            _table = _tableClient.GetTableReference(tableName);
        }

        public async Task TryCreateTable()
        {
            // Create the table if it doesn't exist.
            await _table.CreateIfNotExistsAsync();
        }

        private async Task<List<T>> GetQuery<T>(TableQuery<T> query) where T : TableEntity, new()
        {
            var entities = new List<T>();
            TableContinuationToken token = null;
            do
            {
                var queryResult = await _table.ExecuteQuerySegmentedAsync(query, token);
                entities.AddRange(queryResult.Results);
                token = queryResult.ContinuationToken;
            } while (token != null);
            return entities;
        }

        public async Task<T> Get<T>(string partitionKey, string rowKey) where T : TableEntity, new()
        {
            // Create a retrieve operation that takes a customer entity.
            TableOperation retrieveOperation = TableOperation.Retrieve<T>(partitionKey, rowKey);

            // Execute the retrieve operation.
            TableResult retrievedResult = await _table.ExecuteAsync(retrieveOperation);
            return (T)retrievedResult.Result;
        }

        public async Task<List<T>> GetAll<T>() where T : TableEntity, new()
        {
            return await GetQuery<T>(new TableQuery<T>());
        }

        public async Task<List<T>> GetByPartition<T>(string partitionKey) where T : TableEntity, new()
        {
            TableQuery<T> query = new TableQuery<T>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));

            return await GetQuery<T>(query);
        }

        public async Task Insert(TableEntity entity)
        {
            // Create the TableOperation object that inserts the customer entity.
            TableOperation insertOperation = TableOperation.Insert(entity);

            // Execute the insert operation.
            await _table.ExecuteAsync(insertOperation);
        }

        public async Task InsertBatch(List<TableEntity> entities)
        {
            TableBatchOperation batchOperation = new TableBatchOperation();

            foreach (TableEntity entity in entities)
            {
                batchOperation.Insert(entity);
            }

            // Execute the insert operation.
            await _table.ExecuteBatchAsync(batchOperation);
        }

        public async Task Update(TableEntity entity)
        {
            // Assign the result to a CustomerEntity object.
            TableEntity updateEntity = await Get<TableEntity>(entity.PartitionKey, entity.RowKey);

            if (updateEntity != null)
            {
                // Create the InsertOrReplace TableOperation.
                TableOperation insertOrReplaceOperation = TableOperation.InsertOrReplace(entity);

                // Execute the operation.
                await _table.ExecuteAsync(insertOrReplaceOperation);
            }
            else
            {
                throw new KeyNotFoundException("Couldn't find entity to update");
            }
        }

        public async Task Delete(TableEntity deleteEntity)
        {
            TableOperation deleteOperation = TableOperation.Delete(deleteEntity);

            // Execute the operation.
            await _table.ExecuteAsync(deleteOperation);
        }
    }
}
