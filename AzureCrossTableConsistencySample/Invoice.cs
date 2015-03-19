using System.Collections.Generic;
using System.Linq;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureCrossTableConsistencySample
{
    public class Invoice
    {
        public Invoice()
        {
            Header = new InvoiceHeaderEntity();
            LineItems = new List<InvoiceLineItemEntity>();
        }

        public InvoiceHeaderEntity Header { get; private set; }
        public IList<InvoiceLineItemEntity> LineItems { get; private set; }

        public InvoiceLineItemEntity AddLineItem(string itemId, string desription, decimal quantity, decimal unitPrice, string discountCode = null)
        {
            var lineItem = new InvoiceLineItemEntity
            {
                ItemId = itemId,
                ItemDescription = desription,
                Quantity = quantity,
                UnitPrice = unitPrice,
                Amount = quantity * unitPrice,
                DiscountCode = discountCode,
                InvoiceId = Header.InvoiceId
            };

            LineItems.Add(lineItem);
            return lineItem;
        }

        public bool Save(CloudTableClient tableClient)
        {
            // Protect against bad data.
            if (null == tableClient) return false;

            var invoiceHeaderTable = tableClient.GetTableReference(Header.TableName);
            var addressTable = tableClient.GetTableReference(Header.BillingAddress.TableName);
            var invoiceLineItemTable = LineItems.Count > 0 ? tableClient.GetTableReference(LineItems.First().TableName) : null;

            // Make sure that the target tables exist.
            invoiceHeaderTable.CreateIfNotExists();
            addressTable.CreateIfNotExists();
            if (invoiceLineItemTable != null) invoiceLineItemTable.CreateIfNotExists();

            // Wrap all storage operations into a scope capable of compensating the failed storage operations.
            using (var scope = new CompensationScope())
            {
                // Insert (or update) the invoice header.
                invoiceHeaderTable.Execute(scope, TableOperation.InsertOrReplace(Header));

                // Insert (or update) the billing address.
                addressTable.Execute(scope, TableOperation.InsertOrReplace(Header.BillingAddress));

                // Insert (or update) the shipping address.
                addressTable.Execute(scope, TableOperation.InsertOrReplace(Header.ShippingAddress));

                // Create a batch containing the invoice line items.
                var lineItemBatch = new TableBatchOperation();

                // Make sure we actually possess any line items.
                if (invoiceLineItemTable != null)
                {
                    // Populate the batch with line items to be inserted (or updated).
                    foreach (var lineItem in LineItems)
                    {
                        lineItemBatch.Add(TableOperation.InsertOrReplace(lineItem));
                    }

                    // Insert (or update) the invoice line items.
                    invoiceLineItemTable.Execute(scope, lineItemBatch);
                }

                // Indicate that all operations within the scope are completed successfully.
                scope.Complete();

                return true;
            }
        }
    }
}
