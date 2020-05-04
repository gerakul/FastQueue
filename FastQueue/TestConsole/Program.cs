using FastQueue.Server.Core;
using FastQueue.Server.Core.Model;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace TestConsole
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var topic = new Topic(0, new TopicOptions
            {
                DataArrayOptions = new InfiniteArrayOptions
                {
                    BlockLength = 100000,
                    DataListCapacity = 128,
                    MinimumFreeBlocks = 4
                }
            });

            byte[] buffer = new byte[1000000];
            new Random(DateTimeOffset.UtcNow.Millisecond).NextBytes(buffer);

            var messages = new ReadOnlyMemory<byte>[1000];

            int start = 0;
            int length = 100;
            for (int i = 0; i < messages.Length; i++)
            {
                if (start + length > messages.Length)
                {
                    start = 0;
                }

                messages[i] = buffer.AsMemory(start, length);
                start += length;
            }

            var sw = Stopwatch.StartNew();

            start = 0;
            length = 100;
            for (long i = 0; i < 100_000_000; i += length)
            {
                if (start + length > messages.Length)
                {
                    start = 0;
                }

                topic.Write(messages.AsSpan(start, length));

                start += length;


                if (i % 1000 == 0 && i > 50)
                {
                    topic.FreeTo(i - 50);
                }
            }

            sw.Stop();

            Console.WriteLine($"{sw.ElapsedMilliseconds}");
            Console.WriteLine("end");
            await Task.CompletedTask;
        }
    }
}
