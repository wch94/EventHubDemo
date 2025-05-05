using Azure.Messaging.EventHubs.Consumer;
using Microsoft.Data.SqlClient;
using System.Text;
using System.Text.Json;

string eventHubsConnectionString = "Endpoint=sb://myeventhub-wch94.servicebus.windows.net/;SharedAccessKeyName=myPolicy;SharedAccessKey=redacted;EntityPath=myeventhub-wch94";
string eventHubName = "myeventhub-wch94";
string sqlConnectionString = "Data Source=my-sql-server-wch94.database.windows.net;Initial Catalog=MyDatabase;User ID=wch94_aol.com#EXT#@wch94aol.onmicrosoft.com;Connect Timeout=30;Encrypt=True;Trust Server Certificate=False;Authentication=ActiveDirectoryInteractive;Application Intent=ReadWrite;Multi Subnet Failover=False";

string consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;

await using var consumer = new EventHubConsumerClient(consumerGroup, eventHubsConnectionString, eventHubName);

Console.WriteLine("Listening for events...");

await foreach (PartitionEvent partitionEvent in consumer.ReadEventsAsync())
{
    string eventBody = Encoding.UTF8.GetString(partitionEvent.Data.Body.ToArray());
    Console.WriteLine($"Received: {eventBody}");

    // Example: assuming eventBody is JSON like { "name": "Widget" }
    try
    {
        var json = JsonDocument.Parse(eventBody);
        string? name = json.RootElement.GetProperty("name").GetString();

        if (!string.IsNullOrWhiteSpace(name))
        {
            await InsertProductAsync(sqlConnectionString, name);
            Console.WriteLine($"Inserted product: {name}");
        }
        else
        {
            Console.WriteLine("Invalid event data: missing 'name'");
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Failed to process event: {ex.Message}");
    }
}

static async Task InsertProductAsync(string sqlConnectionString, string name)
{
    using var connection = new SqlConnection(sqlConnectionString);
    await connection.OpenAsync();

    string sql = "INSERT INTO Products (Name) VALUES (@name)";
    using var cmd = new SqlCommand(sql, connection);
    cmd.Parameters.AddWithValue("@name", name);

    await cmd.ExecuteNonQueryAsync();
}
