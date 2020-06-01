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
            //await InfiniteArrayTest();
            //await TopicPerformance();
            await TopicTest();

            Console.WriteLine("end");
            await Task.CompletedTask;
        }

        static async Task TopicTest()
        {
            var storage = new FilePersistentStorage(new FilePersistentStorageOptions
            {
                FileLengthThreshold = 100 * 1024 * 1024,
                DirectoryPath = @"C:\temp\storage",
                NamePrefix = "Data"
            });

            storage.Restore();

            var topic = new Topic(0, "test", storage, new TopicOptions
            {
                PersistenceIntervalMilliseconds = 100,
                DataArrayOptions = new InfiniteArrayOptions
                {
                    BlockLength = 100000,
                    DataListCapacity = 128,
                    MinimumFreeBlocks = 20
                }
            });

            Task.Factory.StartNew(() => topic.PersistenceLoop(), TaskCreationOptions.LongRunning);

            var writer = topic.CreateWriter(async (ack, ct) =>
            {
                Console.WriteLine($"Confirmed {ack.SequenceNumber}. {DateTimeOffset.UtcNow:mm:ss.fffffff}");
                topic.FreeTo(Math.Max(ack.SequenceNumber - 500, 0));
                await Task.CompletedTask;
            });

            long seqNum = 1;
            for (int i = 0; i < 10; i++)
            {
                writer.Write(new WriteRequest(seqNum++, new byte[] { 1, 2, 3, (byte)i }));
            }
            await Task.Delay(500);


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

            start = 0;
            length = 1;
            Console.WriteLine($"Start sending: {DateTimeOffset.UtcNow:mm:ss.fffffff}");

            for (long i = 0; i < 1_000_000; i += length)
            {
                if (start + length > messages.Length)
                {
                    start = 0;
                }

                seqNum += length;
                //writer.Write(new WriteManyRequest(seqNum, messages.AsMemory(start, length)));
                writer.Write(new WriteRequest(seqNum, messages[start]));

                start += length;
            }

            Console.WriteLine($"Stop sending: {DateTimeOffset.UtcNow:mm:ss.fffffff}");

            await Task.Delay(1000);
        }

        static async Task TopicPerformance()
        {
            var topic = new Topic(0, "test", null, new TopicOptions
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
            for (long i = 0; i < 100_000_00; i += length)
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
        }

        static async Task InfiniteArrayTest()
        {
            var data = new InfiniteArray<long>(0, new InfiniteArrayOptions
            {
                BlockLength = 10,
                DataListCapacity = 5,
                MinimumFreeBlocks = 4
            });

            for (long i = 0; i < 127; i++)
            {
                data.Add(i);

                if (i > 8 && i % 5 == 0)
                {
                    data.FreeTo(i - 8);
                }
            }

            data.FreeTo(124);


            var blocks = data.GetDataBlocks();
        }
    }
}
