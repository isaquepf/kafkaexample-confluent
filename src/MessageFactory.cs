using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace poc.confluent.src
{
  public class MessageFactory
  {
    private const string SERVER = "127.0.0.1:9092";
    private const string TOPIC = "VRAL-TOPIC";
    public async Task CreateProducer()
    {
      var config = new ProducerConfig
      {
        BootstrapServers = SERVER
      };

      var builder = new ProducerBuilder<Null, string>(config);

      using (var producer = builder.Build())
      {
        try
        {
          Parallel.For(0, 10
            , async (index) =>
            {
              var message = $"Vral {index}";
              var result = await producer.ProduceAsync(
                  TOPIC,
                  new Message<Null, string> { Value = message });

              Console.WriteLine($"Delivered ({ result.Value}) > ({result.TopicPartitionOffset})");
            });

          await Task.CompletedTask;
        }
        catch (ProduceException<Null, string> exception)
        {
          Console.WriteLine($"Delivery failed: {exception.Error.Reason}");
        }
      }
    }

    public async Task CreateConsumer()
    {
      var config = new ConsumerConfig
      {
        GroupId = "test-consumer",
        BootstrapServers = SERVER,
        AutoOffsetReset = AutoOffsetReset.Earliest
      };

      var builder = new ConsumerBuilder<Ignore, string>(config);

      using (var consumer = builder.Build())
      {

        consumer.Subscribe(TOPIC);

        var cts = new CancellationTokenSource();
        var cancelled = false;
        Console.CancelKeyPress += (_, e) =>
        {
          cancelled = true;
          e.Cancel = true; // prevent the process from terminating.
          cts.Cancel();
        };


        try
        {
          while (!cancelled)
          {
            var result = consumer.Consume(cts.Token);
            Console.WriteLine($"Consume message ({result.Value}) > {result.TopicPartitionOffset}");
          }
        }
        catch (ConsumeException exception)
        {
          Console.WriteLine($"Error occured: {exception.Error.Reason}");
        }
        finally
        {
          consumer.Close();
        }
        await Task.CompletedTask;
      }
    }
  }
}