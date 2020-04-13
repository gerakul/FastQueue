using FastQueue.Server.Core;
using System;
using System.Threading.Tasks;

namespace TestConsole
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var server = new Server(new TopicFactory());

            await server.CreateTopic("aaa");
            await server.CreateTopic("aaa1");
            await server.CreateTopic("aaa");

            Console.WriteLine("end");
            await Task.CompletedTask;
        }
    }
}
