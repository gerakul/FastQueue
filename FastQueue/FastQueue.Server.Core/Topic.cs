using FastQueue.Server.Core.Abstractions;
using FastQueue.Server.Core.Exceptions;
using FastQueue.Server.Core.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
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
        private long lastMessageId;
        private readonly string name;
        private readonly IPersistentStorage persistentStorage;
        private readonly ISubscriptionsConfigurationStorage subscriptionsConfigurationStorage;
        private readonly InfiniteArrayOptions dataArrayOptions;
        private long persistedMessageId;
        private int persistenceIntervalMilliseconds;
        private object dataSync = new object();
        private object writersSync = new object();
        private object subscriptionsSync = new object();
        private CancellationTokenSource cancellationTokenSource;
        private DataSnapshot currentData;
        private long lastFreeToId;

        internal long PersistedMessageId => persistedMessageId;
        internal DataSnapshot CurrentData => currentData;

        internal Topic(string name, IPersistentStorage persistentStorage, ISubscriptionsConfigurationStorage subscriptionsConfigurationStorage,
            TopicOptions topicOptions)
        {
            this.name = name;
            this.persistentStorage = persistentStorage;
            this.subscriptionsConfigurationStorage = subscriptionsConfigurationStorage;
            persistenceIntervalMilliseconds = topicOptions.PersistenceIntervalMilliseconds;
            dataArrayOptions = new InfiniteArrayOptions(topicOptions.DataArrayOptions);
            writers = new HashSet<TopicWriter>();
            subscriptions = new Dictionary<string, Subscription>();
            cancellationTokenSource = new CancellationTokenSource();
            // ::: move to Restore
            lastFreeToId = 1;
        }

        internal TopicWriteResult Write(ReadOnlySpan<ReadOnlyMemory<byte>> messages)
        {
            lock (dataSync)
            {
                var enqueuedTime = DateTime.UtcNow;
                var newMessages = new Message[messages.Length];
                for (int i = 1; i <= newMessages.Length; i++)
                {
                    newMessages[i] = new Message(lastMessageId + i, enqueuedTime, messages[i]);
                }

                var ind = data.Add(newMessages);
                persistentStorage.Write(newMessages.AsSpan());
                lastMessageId += messages.Length;
                return new TopicWriteResult(ind, enqueuedTime);
            }
        }

        internal TopicWriteResult Write(ReadOnlyMemory<byte> message)
        {
            lock (dataSync)
            {
                var enqueuedTime = DateTime.UtcNow;
                var newMessage = new Message(++lastMessageId, enqueuedTime, message);
                var ind = data.Add(newMessage);
                persistentStorage.Write(newMessage);
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

        internal void Restore()
        {
            var restoreEnumerator = persistentStorage.Restore().GetEnumerator();

            if (!restoreEnumerator.MoveNext())
            {
                persistedMessageId = lastMessageId = -1;
                data = new InfiniteArray<Message>(0, dataArrayOptions);
                return;
            }

            data = new InfiniteArray<Message>(restoreEnumerator.Current.ID, dataArrayOptions);
            data.Add(restoreEnumerator.Current);

            while (restoreEnumerator.MoveNext())
            {
                data.Add(restoreEnumerator.Current);
            }

            currentData = new DataSnapshot()
            {
                StartMessageId = data.GetFirstItemIndex(),
                Data = data.GetDataBlocks()
            };

            persistedMessageId = lastMessageId = currentData.Data[^1].Span[^1].ID;

            RestoreSubscriptions();
        }

        internal void FreeTo(long firstValidMessageId)
        {
            lock (dataSync)
            {
                data.FreeTo(firstValidMessageId);
                persistentStorage.FreeTo(firstValidMessageId);
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
            CreateSubscription(subscriptionName, persistedMessageId + 1);
        }

        internal void CreateSubscription(string subscriptionName, long startReadingFromId)
        {
            lock (subscriptionsSync)
            {
                if (subscriptions.ContainsKey(subscriptionName))
                {
                    throw new SubscriptionManagementException($"Subscription {subscriptionName} already exists in the topic {name}");
                }

                subscriptions.Add(subscriptionName, new Subscription(Guid.NewGuid(), subscriptionName, this, startReadingFromId - 1));

                UpdateSubscriptionsConfiguration();
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

                UpdateSubscriptionsConfiguration();
            }
        }

        internal Subscriber Subscribe(string subscriptionName, Func<ReadOnlyMemory<Message>, CancellationToken, Task> push, 
            SubscriberOptions subscriberOptions = null)
        {
            lock (subscriptionsSync)
            {
                Subscription sub;
                if (!subscriptions.TryGetValue(subscriptionName, out sub))
                {
                    throw new SubscriptionManagementException($"Subscription {subscriptionName} doesn't exist in the topic {name}");
                }

                return sub.CreateSubscriber(push, subscriberOptions ?? new SubscriberOptions());
            }
        }

        private void UpdateSubscriptionsConfiguration()
        {
            var newConfig = new SubscriptionsConfiguration
            {
                Subscriptions = subscriptions.Values.Select(x => new SubscriptionConfiguration
                {
                    Id = x.Id,
                    Name = x.Name
                }).ToList()
            };

            subscriptionsConfigurationStorage.Update(newConfig);
        }

        private void RestoreSubscriptions()
        {
            var config = subscriptionsConfigurationStorage.Read();

            foreach (var item in config.Subscriptions)
            {
                // ::: use completedId instead of persistedMessageId
                subscriptions.Add(item.Name, new Subscription(item.Id, item.Name, this, persistedMessageId));
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
                        if (persistedMessageId != lastMessageId)
                        {
                            try
                            {
                                persistentStorage.Flush();
                            }
                            catch
                            {
                                continue;
                            }

                            persistedMessageId = lastMessageId;

                            currentData = new DataSnapshot()
                            {
                                StartMessageId = data.GetFirstItemIndex(),
                                Data = data.GetDataBlocks()
                            };
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

                    var firstNonCompletedId = subscriptions.Values.Min(x => x.CompletedMessageId) + 1;

                    if (lastFreeToId < firstNonCompletedId)
                    {
                        FreeTo(firstNonCompletedId);
                        lastFreeToId = firstNonCompletedId;
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
