using FastQueue.Server.Core.Abstractions;
using FastQueue.Server.Core.Exceptions;
using FastQueue.Server.Core.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace FastQueue.Server.Core
{
    internal class Topic
    {
        private const int CleanupIntervalMilliseconds = 200;

        private InfiniteArray<Message> data;
        private HashSet<TopicWriter> writers;
        private Dictionary<string, Subscription> subscriptions;
        private long offset;
        private readonly string name;
        private readonly IPersistentStorage persistentStorage;
        private long persistedOffset;
        private int persistenceIntervalMilliseconds;
        private object dataSync = new object();
        private object writersSync = new object();
        private object subscriptionsSync = new object();
        private CancellationTokenSource cancellationTokenSource;
        private DataSnapshot currentData;
        private long lastFreeToId;

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
            subscriptions = new Dictionary<string, Subscription>();
            cancellationTokenSource = new CancellationTokenSource();
            lastFreeToId = initialOffset;
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

        internal void Start()
        {
            Task.Factory.StartNew(async () => await PersistenceLoop(cancellationTokenSource.Token), TaskCreationOptions.LongRunning);
            Task.Factory.StartNew(async () => await CleanupLoop(cancellationTokenSource.Token), TaskCreationOptions.LongRunning);
        }

        internal void Stop()
        {
            cancellationTokenSource.Cancel();
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
            lock (subscriptionsSync)
            {
                if (subscriptions.ContainsKey(subscriptionName))
                {
                    throw new SubscriptionManagementException($"Subscription {subscriptionName} already exists in the topic {name}");
                }

                subscriptions.Add(subscriptionName, new Subscription(subscriptionName, this));
            }
        }

        internal void DeleteSubscription(string subscriptionName)
        {
            lock (subscriptionsSync)
            {
                Subscription sub;
                if (!subscriptions.TryGetValue(subscriptionName, out sub))
                {
                    return;
                }

                sub.Dispose();
                subscriptions.Remove(subscriptionName);
            }
        }

        internal Subscriber Subscribe(string subscriptionName, Func<ReadOnlyMemory<Message>, CancellationToken, Task> push)
        {
            lock (subscriptionsSync)
            {
                Subscription sub;
                if (!subscriptions.TryGetValue(subscriptionName, out sub))
                {
                    throw new SubscriptionManagementException($"Subscription {subscriptionName} doesn't exist in the topic {name}");
                }

                return sub.CreateSubscriber(push);
            }
        }

        private async Task PersistenceLoop(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(persistenceIntervalMilliseconds, cancellationToken);

                    lock (dataSync)
                    {
                        if (persistedOffset != offset)
                        {
                            try
                            {
                                persistentStorage.Flush();
                            }
                            catch
                            {
                                continue;
                            }

                            persistedOffset = offset;

                            currentData = new DataSnapshot()
                            {
                                StartMessageId = data.GetFirstItemIndex(),
                                Data = data.GetDataBlocks()
                            };

                            // :::
                            if (currentData.Data.Length > 0 && currentData.StartMessageId != currentData.Data[0].Span[0].ID)
                            {

                            }
                        }
                    }
                }
                catch (TaskCanceledException)
                {
                    break;
                }
            }
        }

        private async Task CleanupLoop(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(CleanupIntervalMilliseconds, cancellationToken);
                }
                catch (TaskCanceledException)
                {
                    break;
                }

                lock (subscriptionsSync)
                {
                    if (subscriptions.Count == 0)
                    {
                        continue;
                    }

                    var minCompletedId = subscriptions.Values.Min(x => x.CompletedMessageId);

                    if (lastFreeToId < minCompletedId)
                    {
                        FreeTo(minCompletedId);
                        lastFreeToId = minCompletedId;
                    }
                }
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
