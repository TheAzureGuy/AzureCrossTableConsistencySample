using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureCrossTableConsistencySample
{
    public class AddressEntity : TableEntity
    {
        public AddressEntity()
        {
            RowKey = Guid.NewGuid().ToString("N");            
        }
        
        public AddressEntity(string customerId) : this()
        {
            PartitionKey = customerId;
        }

        public string AddressId
        {
            get { return RowKey; }
            set { RowKey = value; }
        }

        public string Line1 { get; set; }
        public string Line2 { get; set; }
        public string City { get; set; }
        public string State { get; set; }
        public string Country { get; set; }
        public string PostalCode { get; set; }

        [IgnoreProperty]
        public string TableName { get { return typeof(AddressEntity).Name; } }
    }
}
