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
            var arr = new InfiniteArray<long>(0, new InfiniteArrayOptions
            {
                BlockLength = 100000,
                DataListCapacity = 128,
                MinimumFreeBlocks = 2
            });

            for (long i = 0; i < 1000000; i++)
            {
                arr.Add(i);

                if (i % 100 == 0)
                {
                    arr.FreeTo(i);
                }
            }

            Console.WriteLine("end");
            await Task.CompletedTask;
        }
    }
}
