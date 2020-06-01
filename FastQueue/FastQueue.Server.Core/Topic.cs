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
        private int confirmationIntervalMilliseconds;
        private object dataSync = new object();
        private object writersSync = new object();

        internal Topic(long initialOffset, string name, IPersistentStorage persistentStorage, TopicOptions topicOptions)
        {
            offset = initialOffset;
            persistedOffset = initialOffset;
            this.name = name;
            this.persistentStorage = persistentStorage;
            confirmationIntervalMilliseconds = topicOptions.ConfirmationIntervalMilliseconds;
            data = new InfiniteArray<Message>(initialOffset, topicOptions.DataArrayOptions);
            writers = new HashSet<TopicWriter>();
            subscriptions = new ConcurrentDictionary<string, Subscription>();
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

        public TopicWriteResult Write(ReadOnlyMemory<byte> message)
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

        internal TopicWriter CreateWriter(Func<PublisherAck, Task> ackHandler)
        {
            lock (writersSync)
            {
                var writer = new TopicWriter(this, ackHandler);
                writers.Add(writer);
                return writer;
            }
        }

        internal void DeleteWriter(TopicWriter writer)
        {
            lock (writersSync)
            {
                writers.Remove(writer);
            }
        }

        internal void CreateSubscription(string subscriptionName)
        {
            subscriptions.AddOrUpdate(subscriptionName, subName => new Subscription(subscriptionName),
                (subName, y) => throw new SubscriptionManagementException($"Subscription {subName} already exists in the topic {name}"));
        }

        internal async Task ConfirmationLoop(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                long newPersistedOffset = 0;
                lock (dataSync)
                {
                    if (persistedOffset != offset)
                    {
                        persistentStorage.Flush();
                        persistedOffset = offset;
                        newPersistedOffset = persistedOffset;
                    }
                }

                if (newPersistedOffset > 0)
                {
                    TaskHelper.FireAndForget(() => FireAcks(newPersistedOffset));
                }

                await Task.Delay(confirmationIntervalMilliseconds);
            }
        }

        private void FireAcks(long newPersistedOffset)
        {
            TopicWriter[] writersArr;
            int len;
            lock (writersSync)
            {
                len = writers.Count;
                if (len == 0)
                {
                    return;
                }

                writersArr = ArrayPool<TopicWriter>.Shared.Rent(len);
                writers.CopyTo(writersArr, 0, len);
            }

            try
            {
                for (int i = 0; i < len; i++)
                {
                    var w = writersArr[i];
                    TaskHelper.FireAndForget(() => w.SendAck(newPersistedOffset));
                }
            }
            finally
            {
                ArrayPool<TopicWriter>.Shared.Return(writersArr);
            }
        }
    }

    public class TopicOptions
    {
        public int ConfirmationIntervalMilliseconds { get; set; } = 50;
        public InfiniteArrayOptions DataArrayOptions { get; set; } = new InfiniteArrayOptions();

        public TopicOptions()
        {
        }

        public TopicOptions(TopicOptions options)
        {
            ConfirmationIntervalMilliseconds = options.ConfirmationIntervalMilliseconds;
            DataArrayOptions = new InfiniteArrayOptions(options.DataArrayOptions);
        }
    }
}
