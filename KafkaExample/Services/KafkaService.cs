using KafkaExample.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Confluent.Kafka;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace KafkaExample.Services
{
    internal class KafkaService
    {
        public event EventHandler<Message> MessageReceived;
        ProducerConfig producerConfig = new ProducerConfig() { BootstrapServers = "localhost:9092",Acks = Acks.All };
        ConsumerConfig consumerConfig = new ConsumerConfig() 
        { 
            GroupId = Guid.NewGuid().ToString(), 
            BootstrapServers = "localhost:9092",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        const string topic = "kafka-messenger";

        IProducer<Null, string> _producer;
        IConsumer<Null, string> _consumer;

        CancellationTokenSource cts;
        internal async Task<bool> Connect(string name = "")
        {
            try
            {
                _producer = new ProducerBuilder<Null, string>(producerConfig).Build();
                _consumer = new ConsumerBuilder<Null, string>(consumerConfig).Build();
                _consumer.Subscribe(topic);
                cts = new CancellationTokenSource();
                Console.WriteLine("Connection build Successfully");
                Listening();
                SendMessage(name);
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                return false;
            }
        }

        private void Listening()
        {
            Task.Run(() =>
            {
                while (true)
                {
                    if (cts.IsCancellationRequested)
                        break;
                    try
                    {
                        var result = _consumer.Consume(cts.Token);
                        if (result != null)
                        {
                            var message = JsonSerializer.Deserialize<Message>(result.Value);
                            if (message != null)
                                MessageReceived?.Invoke(this, message);
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex);
                    }
                }
            });
        }

        private async Task SendMessage(string message)
        {
            try
            {
                await _producer.ProduceAsync(topic, new Message<Null, string>() { Value = message });
            }
            catch (Exception ex) { Console.WriteLine(ex.Message); }
        }

        internal async Task SendMessage(Message message)
        {
            await SendMessage(JsonSerializer.Serialize(message));
        }

        internal async Task CloseConnection()
        {
            try
            {
                cts.Cancel();
                _producer.Dispose();
                _consumer.Unsubscribe();
                _consumer.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
    }
}
