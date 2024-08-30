using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace KafkaMessageReader
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("setup Kafka broker connection strings.................." +
                "");
            var config = new ConsumerConfig
            {
                GroupId = "test-consumer-group",
                BootstrapServers = "your-aws-msk-broker-endpoint:9092",  // Replace with your AWS MSK broker endpoint
                AutoOffsetReset = AutoOffsetReset.Earliest,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.ScramSha256, // Replace with your mechanism if different
                SaslUsername = "your-username",  // Replace with your username
                SaslPassword = "your-password"   // Replace with your password
            };

            using (var consumer = new ConsumerBuilder<string, string>(config).Build())
            {
                Console.WriteLine("get Kafka topic...........");
                consumer.Subscribe("your-kafka-topic");  // Replace with your Kafka topic

                int messageCount = 0;
                const int maxMessages = 25;
                Console.WriteLine("start of try catch block.................");
                try
                {
                    while (messageCount < maxMessages)
                    {
                        var consumeResult = consumer.Consume(CancellationToken.None);

                        Console.WriteLine($"Message {messageCount + 1}:");
                        Console.WriteLine($"  Key: {consumeResult.Message.Key}");
                        Console.WriteLine($"  Value: {consumeResult.Message.Value}");

                        messageCount++;
                    }
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Error occurred: {e.Error.Reason}");
                }
                finally
                {
                    Console.WriteLine("Message Count: " + messageCount);
                    consumer.Close();
                }
            }
        }
    }
}

