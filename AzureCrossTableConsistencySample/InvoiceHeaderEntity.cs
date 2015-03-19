using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureCrossTableConsistencySample
{
    public class InvoiceHeaderEntity : TableEntity
    {
        public InvoiceHeaderEntity()
        {
            ShippingAddress = new AddressEntity {AddressId = Guid.NewGuid().ToString("N")};
            ShippingAddressId = ShippingAddress.AddressId;

            BillingAddress = new AddressEntity { AddressId = Guid.NewGuid().ToString("N") };
            BillingAddressId = BillingAddress.AddressId;
        }

        [IgnoreProperty]
        public string CustomerId
        {
            get
            {
                return PartitionKey;
            }
            set
            {
                PartitionKey = value;
                BillingAddress.PartitionKey = value;
                ShippingAddress.PartitionKey = value;
            }
        }

        [IgnoreProperty]
        public string InvoiceId
        {
            get { return RowKey; }
            set { RowKey = value; }
        }

        public string PurchaseOrderNumber { get; set; }
        public string Currency { get; set; }
        public DateTime InvoiceDate { get; set; }
        public DateTime ShipDate { get; set; }
        public string ShippingAddressId { get; set; }
        public string BillingAddressId { get; set; }

        [IgnoreProperty]
        public AddressEntity ShippingAddress { get; private set; }

        [IgnoreProperty]
        public AddressEntity BillingAddress { get; private set; }

        [IgnoreProperty]
        public string TableName { get { return typeof (InvoiceHeaderEntity).Name; } }
    }
}
