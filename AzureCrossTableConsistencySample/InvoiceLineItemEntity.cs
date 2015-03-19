using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureCrossTableConsistencySample
{
    public class InvoiceLineItemEntity : TableEntity
    {
        [IgnoreProperty]
        public string InvoiceId
        {
            get { return PartitionKey; }
            set { PartitionKey = value; }
        }

        [IgnoreProperty]
        public string ItemId
        {
            get { return RowKey; }
            set { RowKey = value; }
        }

        public string ItemDescription { get; set; }
        public decimal Quantity { get; set; }
        public decimal UnitPrice { get; set; }
        public decimal Amount { get; set; }
        public string DiscountCode { get; set; }

        [IgnoreProperty]
        public string TableName { get { return typeof(InvoiceLineItemEntity).Name; } }
    }
}
