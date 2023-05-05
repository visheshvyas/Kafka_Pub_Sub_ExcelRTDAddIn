using Confluent.Kafka;
using Microsoft.Office.Interop.Excel;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ExcelAddin_NetFW
{
    [Guid("7e081228-bf65-488b-93f1-d653f8ca52fe")]
    [ProgId("hp_rtdserver.rtdserver.progid")]
    [ComVisible(true)]
    public class HPRTDServer : IRtdServer
    {
        // Excel callback object to notify Excel of updates.
        private IRTDUpdateEvent m_callback = null;

        // Dictionary that stores for each TopicID the HPRtdServerTopic object.
        private Dictionary<int, HPRtdServerTopic> m_topics = new Dictionary<int, HPRtdServerTopic>();

        public int ServerStart(IRTDUpdateEvent CallbackObject)
        {
            // Store the callback object.
            m_callback = CallbackObject;
            return 1;
        }

        public object ConnectData(int TopicID, ref Array Symbols, ref bool GetNewValues)
        {
            try
            {
                // Add a topic object if not already in the dictionary.
                if (!m_topics.ContainsKey(TopicID))
                {
                    //we are expecting only one Symbol at a time in the RTD Call
                    m_topics.Add(TopicID, new HPRtdServerTopic(Symbols.GetValue(0).ToString()));

                    Task tsk = new Task(() =>
                    {
                        KafkaConsumerForExcel kafkaConsumer = new KafkaConsumerForExcel(m_topics, m_callback);
                        kafkaConsumer.BuildAndSubscribe();
                    });
                    tsk.Start();
                }

                // Excel should retrieve new values.
                GetNewValues = true;

                // Return the current data for this topic.
                return m_topics[TopicID].GetData();
            }
            catch (Exception ex)
            {
                return ex.Message;
            }
        }

        public Array RefreshData(ref int TopicCount)
        {
            // Return for each topic, the topicID and new data.
            object[,] data = new object[2, m_topics.Count];

            try
            {
                // Get data for each topic.
                int index = 0;
                foreach (int topicId in m_topics.Keys)
                {
                    data[0, index] = topicId;
                    data[1, index] = m_topics[topicId].GetData();
                    index++;
                }
                // Set the topic count
                TopicCount = m_topics.Count;
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            // Return the data.
            return data;
        }

        public void DisconnectData(int TopicID)
        {
            // Remove the topic from the dictionary.
            if (m_topics.ContainsKey(TopicID))
            {
                m_topics[TopicID].Dispose();
                m_topics.Remove(TopicID);
            }
        }

        // call by Excel every 15 seconds to keep server and connection alive
        public int Heartbeat()
        {
            // Always return OK.
            return 1;
        }
        public void ServerTerminate()
        {
            //incase of server terminate we should dispose
            foreach (int topicId in m_topics.Keys)
            {
                m_topics[topicId].Dispose();
                m_topics.Remove(topicId);
            }                
        }

    }

    public class HPRtdServerTopic : IDisposable
    {
        public HPRtdServerTopic(string symbol)
        {
            topicSymbol = symbol;
        }
        public object GetData()
        {
            return topicPrice;
        }
        public void SetData(decimal price)
        {
            topicPrice = price;
        }
        public string topicSymbol { get; set; }
        public decimal topicPrice { get; set; }
        public void Dispose()
        {
            this.Dispose();
        }
    }

    public class KafkaConsumerForExcel
    {
        ConsumerConfig config = null;
        Dictionary<int, HPRtdServerTopic> m_topics;
        IRTDUpdateEvent ExcelCallBack;

        public KafkaConsumerForExcel(Dictionary<int, HPRtdServerTopic> topics, IRTDUpdateEvent excelCallBack)
        {
            config = new ConsumerConfig
            {
                //Required connection configs for Kafka producer, consumer, and admin
                BootstrapServers = "pkc-56d1g.eastus.azure.confluent.cloud:9092",
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "MDA4E3XPHDJYIEE6",
                SaslPassword = "QUCrLukykSWhZuysjQno27oy4bIgeVvkkdAGvemTZXQ23dEujV9jDf0i6dlHEhdU",

                GroupId = Guid.NewGuid().ToString(),//this is required to identify the consumer.
            };
            m_topics = topics;
            ExcelCallBack = excelCallBack;
        }

        public void BuildAndSubscribe()
        {
            var consumer = new ConsumerBuilder<string, string>(config).Build();
            consumer.Subscribe("test-topic");
            CancellationTokenSource cts = new CancellationTokenSource();

            try
            {
                while (true)
                {
                    var response = consumer.Consume(cts.Token);
                    if (response.Message != null)
                    {
                        var kfaka_response_data = JsonConvert.DeserializeObject<TestData>(response.Message.Value);
                        
                        if (kfaka_response_data != null)
                        {
                            foreach (var item in m_topics)
                            {
                                if(item.Value.topicSymbol == kfaka_response_data.Symbol)
                                    item.Value.SetData(kfaka_response_data.Price);
                            }

                            //the below call with send the callback to the excel that the new data is available for refresh
                            //after this its Excel that will call RefreshData to get the updated data
                            ExcelCallBack.UpdateNotify();
                        }
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

    public class TestData
    {
        public string Symbol { get; set; }
        public DateTime GeneratedDateTime { get; set; }
        public decimal Price { get; set; }
    }
}
