using System;
using System.Threading.Tasks;
using poc.confluent.src;

namespace poc.confluent
{
  class Program
  {
    static async Task Main(string[] args)
    {
      Console.WriteLine("Ctrl-C to exit.");

      var message = new MessageFactory();

      await message.CreateProducer();

      await message.CreateConsumer();
    }
  }
}
