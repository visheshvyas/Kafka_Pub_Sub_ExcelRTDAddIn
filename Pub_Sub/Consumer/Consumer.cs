using Confluent.Kafka;
using HPCommon;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using System.Data.SqlClient;

namespace Consumer
{
    internal class Consumer
    {
        static void Main(string[] args)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(AppDomain.CurrentDomain.BaseDirectory)
                .AddJsonFile("config.json", optional: false);
            var builderconfig = builder.Build();

            //template insert query
            string insertQuery = builderconfig["InsertQuery"];

            var config = new ConsumerConfig
            {
                //Required connection configs for Kafka producer, consumer, and admin
                BootstrapServers = builderconfig["BootstrapServers"],
                SaslUsername = builderconfig["SaslUsername"],
                SaslPassword = builderconfig["SaslPassword"],
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                GroupId = Guid.NewGuid().ToString(),//this is required to identify the consumer.
            };

            Console.WriteLine("Starting Consumer...");

            using var consumer = new ConsumerBuilder<string, string>(config).Build();
            consumer.Subscribe(builderconfig["kafka_topic"]);
            CancellationTokenSource cts = new CancellationTokenSource();

            try
            {
                while (true)
                {
                    var response = consumer.Consume(cts.Token);
                    if (response.Message != null)
                    {
                        var topic_data = JsonConvert.DeserializeObject<HPData.TestData>(response.Message.Value);
                        Console.WriteLine($"Key : {response.Message.Key}, Time : {topic_data?.GeneratedDateTime.Ticks}, Value : {topic_data?.Price}");

                        Task taskDB = Task.Run(() =>
                        {
                            //inserting the response recived from the topic into database
                            using (SqlConnection sqlConn = new SqlConnection(builderconfig["SqlConnString"]))
                            {
                                using (SqlCommand cmd = new SqlCommand())
                                {
                                    try
                                    {
                                        cmd.CommandType = System.Data.CommandType.Text;
                                        cmd.CommandText = string.Format(insertQuery, response.Message.Key, topic_data?.GeneratedDateTime.Ticks, topic_data?.Price);
                                        cmd.Connection = sqlConn;

                                        sqlConn.Open();
                                        cmd.ExecuteNonQuery();
                                    }
                                    catch (Exception ex)
                                    {
                                        Console.WriteLine("Exception while inserting record into database : " + ex.Message);
                                        Console.WriteLine("Sql Query : " + cmd.CommandText);
                                    }
                                }
                            }
                        });
                    }
                }
            }
            catch (ConsumeException exc)
            {
                cts.Cancel();//this is to notify the cancellation
                Console.WriteLine(exc.Message);
            }
        }
    }
}