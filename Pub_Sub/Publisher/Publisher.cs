using Confluent.Kafka;
using Newtonsoft.Json;
using static HPCommon.HPData;
using Microsoft.Extensions.Configuration;
using System.Diagnostics;

namespace Publisher
{
    internal class Publisher
    {
        static Random rnd = new Random();
        static int minKeyId = 1;
        static int maxKeyId = 3;
        static ProducerConfig? config;
        static IProducer<string, string>? producer;
        static string kafka_topic = string.Empty;
        static void Main(string[] args)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(AppDomain.CurrentDomain.BaseDirectory)
                .AddJsonFile("config.json", optional: false);
            var builderconfig = builder.Build();
            config = new ProducerConfig
            {
                //Required connection configs for Kafka producer, consumer, and admin
                BootstrapServers = builderconfig["BootstrapServers"],
                SaslUsername = builderconfig["SaslUsername"],
                SaslPassword = builderconfig["SaslPassword"],
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                Acks = Acks.Leader, //only ack from leader is enough in this case
            };

            if(!string.IsNullOrEmpty(builderconfig["maxKeyId"]))
                int.TryParse(builderconfig["maxKeyId"],out maxKeyId);

            kafka_topic = builderconfig["kafka_topic"];

            Console.WriteLine("Starting Publisher...");
            try
            {
                producer = new ProducerBuilder<string, string>(config).Build();
                while (true && producer !=null)
                {
                    Stopwatch sw = Stopwatch.StartNew();
                    GenerateAndSendMessage(2);//generating and sending 2 message
                    sw.Stop();
                    if(sw.ElapsedMilliseconds < 1000) //waiting for 1 sec altogether before publishing another messages
                        Thread.Sleep((int)(1000 - sw.ElapsedMilliseconds));//waiting for 10 sec
                }
            }
            catch (ProduceException<Null, string> exc)
            {
                Console.WriteLine(exc.Message);
            }
        }

        static async void GenerateAndSendMessage(int times)
        {
            for (int i = 0; i < times; i++)
            {
                //this is to generate the key e.g. k1 or k2
                //to increase the key to k3, k4, k5.....kn, just update the maxKeyId to n+1
                string _key = "k" + rnd.Next(minKeyId, maxKeyId);

                //the below code is to generate the data that needs to be sent by the publisher
                var testData = new TestData(_key, DateTime.Now, Convert.ToDecimal(rnd.NextDouble()));

                if (producer != null)
                {
                    //sending the data to kafka
                    var response = await producer.ProduceAsync(kafka_topic,
                        new Message<string, string>
                        {
                            Key = testData.Symbol,
                            Value = JsonConvert.SerializeObject(testData)
                        });

                    //printing out what was sent in the last message.
                    Console.WriteLine($"{_key} => {testData.GeneratedDateTime.Ticks} => {testData.Price.ToString()}");
                }
                else
                {
                    Console.WriteLine("Instance of producer is NULL. Please check.");
                }
            }
        }

    }
}