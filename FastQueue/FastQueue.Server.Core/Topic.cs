using FastQueue.Server.Core.Abstractions;
using FastQueue.Server.Core.Exceptions;
using FastQueue.Server.Core.Model;
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FastQueue.Server.Core
{
    internal class Topic
    {
        private InfiniteArray<Message> data;
        private HashSet<TopicWriter> writers;
        private ConcurrentDictionary<string, Subscription> subscriptions;
        private long offset;
        private readonly string name;
        private readonly IPersistentStorage persistentStorage;
        private long persistedOffset;
        private int persistenceIntervalMilliseconds;
        private object dataSync = new object();
        private object writersSync = new object();
        private CancellationTokenSource cancellationTokenSource;
        private DataSnapshot currentData;

        internal long PersistedOffset => persistedOffset;
        internal DataSnapshot CurrentData => currentData;

        internal Topic(long initialOffset, string name, IPersistentStorage persistentStorage, TopicOptions topicOptions)
        {
            offset = initialOffset;
            persistedOffset = initialOffset;
            this.name = name;
            this.persistentStorage = persistentStorage;
            persistenceIntervalMilliseconds = topicOptions.PersistenceIntervalMilliseconds;
            data = new InfiniteArray<Message>(initialOffset, topicOptions.DataArrayOptions);
            writers = new HashSet<TopicWriter>();
            subscriptions = new ConcurrentDictionary<string, Subscription>();
            cancellationTokenSource = new CancellationTokenSource();
        }

        internal TopicWriteResult Write(ReadOnlySpan<ReadOnlyMemory<byte>> messages)
        {
            lock (dataSync)
            {
                var enqueuedTime = DateTime.UtcNow;
                var newMessages = new Message[messages.Length];
                for (int i = 0; i < newMessages.Length; i++)
                {
                    newMessages[i] = new Message(offset + i, enqueuedTime, messages[i]);
                }

                var ind = data.Add(newMessages);
                persistentStorage.Write(newMessages.AsSpan());
                offset += messages.Length;
                return new TopicWriteResult(ind, enqueuedTime);
            }
        }

        internal TopicWriteResult Write(ReadOnlyMemory<byte> message)
        {
            lock (dataSync)
            {
                var enqueuedTime = DateTime.UtcNow;
                var newMessage = new Message(offset, enqueuedTime, message);
                var ind = data.Add(newMessage);
                persistentStorage.Write(newMessage);
                offset++;
                return new TopicWriteResult(ind, enqueuedTime);
            }
        }

        internal void FreeTo(long offset)
        {
            lock (dataSync)
            {
                data.FreeTo(offset);
                persistentStorage.FreeTo(offset);
            }
        }

        internal TopicWriter CreateWriter(Func<PublisherAck, CancellationToken, Task> ackHandler)
        {
            lock (writersSync)
            {
                var writer = new TopicWriter(this, ackHandler);
                writers.Add(writer);
                writer.StartConfirmationLoop();
                return writer;
            }
        }

        internal void DeleteWriter(TopicWriter writer)
        {
            lock (writersSync)
            {
                writer.StopConfirmationLoop();
                writers.Remove(writer);
            }
        }

        internal void CreateSubscription(string subscriptionName)
        {
            subscriptions.AddOrUpdate(subscriptionName, subName => new Subscription(subscriptionName, this),
                (subName, y) => throw new SubscriptionManagementException($"Subscription {subName} already exists in the topic {name}"));
        }

        internal async Task PersistenceLoop()
        {
            while (!cancellationTokenSource.IsCancellationRequested)
            {
                lock (dataSync)
                {
                    if (persistedOffset != offset)
                    {
                        persistentStorage.Flush();
                        persistedOffset = offset;

                        currentData = new DataSnapshot()
                        {
                            StartMessageId = data.GetFirstItemIndex(),
                            Data = data.GetDataBlocks()
                        };
                    }
                }

                await Task.Delay(persistenceIntervalMilliseconds);
            }
        }
    }

    public class TopicOptions
    {
        public int PersistenceIntervalMilliseconds { get; set; } = 50;
        public InfiniteArrayOptions DataArrayOptions { get; set; } = new InfiniteArrayOptions();

        public TopicOptions()
        {
        }

        public TopicOptions(TopicOptions options)
        {
            PersistenceIntervalMilliseconds = options.PersistenceIntervalMilliseconds;
            DataArrayOptions = new InfiniteArrayOptions(options.DataArrayOptions);
        }
    }
}
