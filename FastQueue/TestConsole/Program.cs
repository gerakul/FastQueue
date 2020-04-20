using FastQueue.Server.Core;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace TestConsole
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var arr = new InfiniteArray<int>(1000, new InfiniteArrayOptions
            {
                BlockLength = 10,
                DataListCapacity = 5,
                MinimumFreeBlocks = 2
            });

            var items = Enumerable.Range(0, 10000).ToArray();

            arr.Add(items.AsSpan(0, 100));
            arr.FreeTo(1025);
            arr.Add(items.AsSpan(100, 100));
            arr.FreeTo(1190);
            arr.Add(items.AsSpan(200, 97));
            arr.FreeTo(1190);
            arr.Add(items.AsSpan(200, 97));
            arr.FreeTo(1190);
            arr.Add(items.AsSpan(200, 97));
            arr.FreeTo(1190);
            arr.Add(items.AsSpan(200, 97));
            arr.FreeTo(1490);
            arr.Add(items.AsSpan(200, 97));

            Console.WriteLine("end");
            await Task.CompletedTask;
        }
    }
}
