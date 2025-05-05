using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using System.Text;

// Your Event Hub connection string and name
string connectionString = "Endpoint=sb://myeventhub-wch94.servicebus.windows.net/;SharedAccessKeyName=myPolicy;SharedAccessKey=redacted;EntityPath=myeventhub-wch94";
string eventHubName = "myeventhub-wch94";

await using var producerClient = new EventHubProducerClient(connectionString, eventHubName);

using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

for (int i = 1; i <= 100; i++)
{
    string messageJson = $"{{ \"name\": \"Product {i}\" }}";
    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(messageJson)));
    Console.WriteLine($"Added message: {messageJson}");
}

await producerClient.SendAsync(eventBatch);

Console.WriteLine("Batch sent!");
