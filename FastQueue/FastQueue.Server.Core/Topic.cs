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
    internal class Topic : ITopicManagement
    {
        private const int CleanupIntervalMilliseconds = 200;
        private const int SubscriptionPointersFlushIntervalMilliseconds = 200;

        private InfiniteArray<Message> data;
        private HashSet<TopicWriter> writers;
        private Dictionary<string, Subscription> subscriptions;
        private long lastMessageId;
        private readonly string name;
        private readonly IPersistentStorage persistentStorage;
        private readonly ISubscriptionsConfigurationStorage subscriptionsConfigurationStorage;
        private readonly ISubscriptionPointersStorage subscriptionPointersStorage;
        private readonly InfiniteArrayOptions dataArrayOptions;
        private long persistedMessageId;
        private int persistenceIntervalMilliseconds;
        private int persistenceMaxFails;
        private int cleanupMaxFails;
        private int subscriptionPointersFlushMaxFails;
        private object dataSync = new object();
        private object writersSync = new object();
        private object subscriptionsSync = new object();
        private CancellationTokenSource cancellationTokenSource;
        private DataSnapshot currentData;
        private long lastFreeToId;
        private int persistenceFails;
        private int cleanupFails;
        private int subscriptionPointersFlushFails;
        private bool stopping;
        private Task<Task> persistenceLoopTask;
        private Task<Task> cleanupLoopTask;
        private Task<Task> subscriptionPointersFlushLoopTask;

        internal long PersistedMessageId => persistedMessageId;
        internal DataSnapshot CurrentData => currentData;
        public string Name => name;

        internal Topic(string name, IPersistentStorage persistentStorage, 
            ISubscriptionsConfigurationStorage subscriptionsConfigurationStorage,
            ISubscriptionPointersStorage subscriptionPointersStorage,
            TopicOptions topicOptions)
        {
            this.name = name;
            this.persistentStorage = persistentStorage;
            this.subscriptionsConfigurationStorage = subscriptionsConfigurationStorage;
            this.subscriptionPointersStorage = subscriptionPointersStorage;
            persistenceIntervalMilliseconds = topicOptions.PersistenceIntervalMilliseconds;
            persistenceMaxFails = topicOptions.PersistenceMaxFails;
            cleanupMaxFails = topicOptions.CleanupMaxFails;
            subscriptionPointersFlushMaxFails = topicOptions.SubscriptionPointersFlushMaxFails;
            dataArrayOptions = new InfiniteArrayOptions(topicOptions.DataArrayOptions);
            writers = new HashSet<TopicWriter>();
            subscriptions = new Dictionary<string, Subscription>();
            cancellationTokenSource = new CancellationTokenSource();
            lastFreeToId = 1;
            stopping = false;
            persistenceFails = 0;
            cleanupFails = 0;
            subscriptionPointersFlushFails = 0;
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
                persistentStorage.Write(newMessages);
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

        public void Start()
        {
            persistenceLoopTask = Task.Factory.StartNew(() => PersistenceLoop(cancellationTokenSource.Token), TaskCreationOptions.LongRunning);
            cleanupLoopTask =  Task.Factory.StartNew(() => CleanupLoop(cancellationTokenSource.Token), TaskCreationOptions.LongRunning);
            subscriptionPointersFlushLoopTask = Task.Factory.StartNew(() => SubscriptionPointersFlushLoop(cancellationTokenSource.Token), TaskCreationOptions.LongRunning);
        }

        public async Task Stop(bool checkForSubscriptions)
        {
            List<TopicWriter> writersList;
            List<Subscription> subscriptionsList;
            lock (writersSync)
            {
                lock (subscriptionsSync)
                {
                    stopping = true;
                    subscriptionsList = subscriptions.Values.ToList();

                    if (checkForSubscriptions && subscriptionsList.Count > 0)
                    {
                        throw new TopicManagementException($"Cannot stop topic with subscriptions");
                    }
                }

                writersList = writers.ToList();
            }

            foreach (var w in writersList)
            {
                await w.DisposeAsync();
            }

            foreach (var s in subscriptionsList)
            {
                await s.DisposeAsync();
            }

            cancellationTokenSource.Cancel();

            await await persistenceLoopTask;
            await await cleanupLoopTask;
            await await subscriptionPointersFlushLoopTask;

            PersistenceAction();
            CleanupAction();
            SubscriptionPointersFlushAction();

            persistentStorage.Dispose();
            subscriptionPointersStorage.Dispose();
        }

        public void Restore()
        {
            var restoreEnumerator = persistentStorage.Restore().GetEnumerator();

            if (!restoreEnumerator.MoveNext())
            {
                persistedMessageId = lastMessageId = -1;
                data = new InfiniteArray<Message>(0, dataArrayOptions);
                RestoreSubscriptions();
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

        public ITopicWriter CreateWriter(Func<PublisherAck, CancellationToken, Task> ackHandler, TopicWriterOptions topicWriterOptions = null)
        {
            lock (writersSync)
            {
                if (stopping)
                {
                    throw new TopicManagementException($"Cannot create Writer when topic {name} is being stopped");
                }

                var writer = new TopicWriter(this, ackHandler, topicWriterOptions ?? new TopicWriterOptions());
                writers.Add(writer);
                writer.StartConfirmationLoop();
                return writer;
            }
        }

        internal async Task DeleteWriter(TopicWriter writer)
        {
            lock (writersSync)
            {
                writers.Remove(writer);
            }     
            
            await writer.StopConfirmationLoop();
        }

        public void CreateSubscription(string subscriptionName)
        {
            CreateSubscription(subscriptionName, persistedMessageId + 1);
        }

        public void CreateSubscription(string subscriptionName, long startReadingFromId)
        {
            lock (subscriptionsSync)
            {
                if (stopping)
                {
                    throw new TopicManagementException($"Cannot create subscription when topic {name} is being stopped");
                }

                if (subscriptions.ContainsKey(subscriptionName))
                {
                    throw new SubscriptionManagementException($"Subscription {subscriptionName} already exists in the topic {name}");
                }

                subscriptions.Add(subscriptionName, new Subscription(Guid.NewGuid(), subscriptionName, this, startReadingFromId - 1,
                    subscriptionPointersStorage));

                UpdateSubscriptionsConfiguration();
            }
        }

        public async Task DeleteSubscription(string subscriptionName)
        {
            Subscription sub;
            lock (subscriptionsSync)
            {
                if (!subscriptions.TryGetValue(subscriptionName, out sub))
                {
                    return;
                }

                subscriptions.Remove(subscriptionName);

                UpdateSubscriptionsConfiguration();
            }

            await sub.DisposeAsync();
        }

        public bool SubscriptionExists(string subscriptionName)
        {
            lock (subscriptionsSync)
            {
                return subscriptions.ContainsKey(subscriptionName);
            }
        }

        public string[] GetSubscriptions()
        {
            lock (subscriptionsSync)
            {
                return subscriptions.Keys.ToArray();
            }
        }

        public ISubscriber Subscribe(string subscriptionName, Func<ReadOnlyMemory<Message>, CancellationToken, Task> push, 
            SubscriberOptions subscriberOptions = null)
        {
            lock (subscriptionsSync)
            {
                if (stopping)
                {
                    throw new SubscriptionManagementException($"Cannot subscribe when topic {name} is being stopped");
                }

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
            var pointers = subscriptionPointersStorage.Restore(GetCurrentCompletedIds);

            foreach (var item in config.Subscriptions)
            {
                long completedId;
                if (!pointers.TryGetValue(item.Id, out completedId))
                {
                    completedId = persistedMessageId;
                }

                subscriptions.Add(item.Name, new Subscription(item.Id, item.Name, this, completedId, subscriptionPointersStorage));
            }
        }

        private async Task PersistenceLoop(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(persistenceIntervalMilliseconds, cancellationToken);
                }
                catch (TaskCanceledException)
                {
                    break;
                }

                try
                {
                    PersistenceAction();
                    persistenceFails = 0;
                }
                catch
                {
                    if ((++persistenceFails) >= persistenceMaxFails)
                    {
                        TaskHelper.FireAndForget(async () => await Stop(false));
                        // ::: logging instead of throwing
                        throw;
                    }
                }
            }
        }

        private void PersistenceAction()
        {
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
                        return;
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

                try
                {
                    CleanupAction();
                    cleanupFails = 0;
                }
                catch
                {
                    if ((++cleanupFails) >= cleanupMaxFails)
                    {
                        TaskHelper.FireAndForget(async () => await Stop(false));
                        // ::: logging instead of throwing
                        throw;
                    }
                }
            }
        }

        private void CleanupAction()
        {
            lock (subscriptionsSync)
            {
                if (subscriptions.Count == 0)
                {
                    return;
                }

                var firstNonCompletedId = subscriptions.Values.Min(x => x.CompletedMessageId) + 1;

                if (lastFreeToId < firstNonCompletedId)
                {
                    FreeTo(firstNonCompletedId);
                    lastFreeToId = firstNonCompletedId;
                }
            }
        }

        private async Task SubscriptionPointersFlushLoop(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(SubscriptionPointersFlushIntervalMilliseconds, cancellationToken);
                }
                catch (TaskCanceledException)
                {
                    break;
                }

                try
                {
                    SubscriptionPointersFlushAction();
                    subscriptionPointersFlushFails = 0;
                }
                catch
                {
                    if ((++subscriptionPointersFlushFails) >= subscriptionPointersFlushMaxFails)
                    {
                        TaskHelper.FireAndForget(async () => await Stop(false));
                        // ::: logging instead of throwing
                        throw;
                    }
                }
            }
        }

        private void SubscriptionPointersFlushAction()
        {
            subscriptionPointersStorage.Flush();
        }

        private Dictionary<Guid, long> GetCurrentCompletedIds()
        {
            lock (subscriptionsSync)
            {
                return subscriptions.Values.ToDictionary(x => x.Id, x => x.CompletedMessageId);
            }
        }

        private void FreeTo(long firstValidMessageId)
        {
            lock (dataSync)
            {
                data.FreeTo(firstValidMessageId);
                persistentStorage.FreeTo(firstValidMessageId);
            }
        }
    }

    public class TopicOptions
    {
        public int PersistenceIntervalMilliseconds { get; set; } = 50;
        public int PersistenceMaxFails { get; set; } = 100;
        public int CleanupMaxFails { get; set; } = 10000;
        public int SubscriptionPointersFlushMaxFails { get; set; } = 500;
        public InfiniteArrayOptions DataArrayOptions { get; set; } = new InfiniteArrayOptions();

        public TopicOptions()
        {
        }

        public TopicOptions(TopicOptions options)
        {
            PersistenceIntervalMilliseconds = options.PersistenceIntervalMilliseconds;
            PersistenceMaxFails = options.PersistenceMaxFails;
            CleanupMaxFails = options.CleanupMaxFails;
            SubscriptionPointersFlushMaxFails = options.SubscriptionPointersFlushMaxFails;
            DataArrayOptions = new InfiniteArrayOptions(options.DataArrayOptions);
        }
    }
}
