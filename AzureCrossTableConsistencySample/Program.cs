using System;
using System.Linq;
using System.Configuration;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureCrossTableConsistencySample
{
    class Program
    {
        static void Main(string[] args)
        {
            // Set up a table client that provides access to the Azure storage table.
            var storageAccount = CloudStorageAccount.Parse(ConfigurationManager.AppSettings["StorageConnectionString"]);
            var tableClient = storageAccount.CreateCloudTableClient();

            // Create a sample valid invoice that can be successfully saved into the storage table.
            var validInvoice = CreateSampleInvoice();

            // Persist the invoice data into the Azure table.
            SaveInvoice(tableClient, validInvoice);

            // Create another sample invoice which we will intentionally damage.
            var invalidInvoice = CreateSampleInvoice();

            // Add a line item with an intentionally large description that does not meet the Azure table constraints.
            invalidInvoice.AddLineItem("BAD//DATA", "White 24HP-LED Tower G4 Lamp", 5m, 19.95m);

            // Persist the second invoice data into the Azure table.
            SaveInvoice(tableClient, invalidInvoice);

            Console.ReadLine();
        }

        private static bool EntityExists(CloudTableClient tableClient, string tableName, string pk, string rk)
        {
            var storageTable = tableClient.GetTableReference(tableName);
            var tableQuery = storageTable.CreateQuery<DynamicTableEntity>().Where(e => e.PartitionKey == pk && e.RowKey == rk).Take(1);

            return tableQuery.FirstOrDefault() != null;
        }

        private static void SaveInvoice(CloudTableClient tableClient, Invoice invoice)
        {
            try
            { 
                // Persist the invoice data into the Azure table.
                invoice.Save(tableClient);

                // Write a helpful diagnostic message.
                Console.WriteLine("Invoice {0} was successfully created", invoice.Header.InvoiceId);
            }
            catch(Exception ex)
            {
                // Write a helpful diagnostic message.
                Console.WriteLine("Invoice {0} has failed to be created. Reason: {1}", invoice.Header.InvoiceId, ex.Message);
            }

            Console.WriteLine("Invoice {0} does {1}exist in the table", invoice.Header.InvoiceId, EntityExists(tableClient, invoice.Header.TableName, invoice.Header.PartitionKey, invoice.Header.RowKey) ? "" : "NOT ");
            Console.WriteLine();
        }

        private static Invoice CreateSampleInvoice()
        {
            // Create a blank invoice.
            var invoice = new Invoice();

            // Populate the blank invoice with sample data.
            invoice.Header.CustomerId = Guid.NewGuid().ToString("N");
            invoice.Header.InvoiceId = DateTime.UtcNow.Ticks.ToString();
            invoice.Header.Currency = "USD";
            invoice.Header.InvoiceDate = DateTime.UtcNow;
            invoice.Header.PurchaseOrderNumber = String.Format("PO{0}", DateTime.UtcNow.Millisecond);
            invoice.Header.ShipDate = DateTime.UtcNow.AddDays(3);

            // Add the shipping address information.
            invoice.Header.ShippingAddress.Line1 = "123 Main Street";
            invoice.Header.ShippingAddress.City = "Tacoma";
            invoice.Header.ShippingAddress.State = "WA";
            invoice.Header.ShippingAddress.PostalCode = "98401";
            invoice.Header.ShippingAddress.Country = "USA";

            // Add the billing address information.
            invoice.Header.BillingAddress.Line1 = "789 Ocean Drive";
            invoice.Header.BillingAddress.City = "Miami";
            invoice.Header.BillingAddress.State = "FL";
            invoice.Header.BillingAddress.PostalCode = "33101";
            invoice.Header.BillingAddress.Country = "USA";

            // Add the line items.
            invoice.AddLineItem("G4-xWHP24-TAC", "White 24HP-LED Tower G4 Lamp", 5m, 19.95m);
            invoice.AddLineItem("G4-xHP45-TAC", "LED Tower G4 Lamp, 45 High Power LEDs", 1m, 19.95m);
            invoice.AddLineItem("G4-WHP", "Cool White 1HP-LED G4 Lamp", 10m, 5.95m);
            invoice.AddLineItem("G4-xW2W-CTAC", "LED Ceramic Tower G4 Lamp, 3 High Power LEDs", 10m, 9.95m);
            invoice.AddLineItem("G4B-xHP21-DAC", "LED G4 Lamp, 21 High Power LED Disc Type with Back Pins", 10m, 11.95m, "CYBERFRIDAY10");

            return invoice;
        }
    }
}
