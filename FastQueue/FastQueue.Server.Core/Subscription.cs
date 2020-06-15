using FastQueue.Server.Core.Abstractions;
using FastQueue.Server.Core.Exceptions;
using FastQueue.Server.Core.Model;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace FastQueue.Server.Core
{
    internal class Subscription : IDisposable
    {
        private Guid id;
        private string name;
        private Topic topic;
        private long completedMessageId;
        private readonly ISubscriptionPointersStorage subscriptionPointersStorage;
        private Subscriber subscriber;
        private object sync = new object();

        internal Guid Id => id;
        internal string Name => name;
        internal Topic Topic => topic;
        internal long CompletedMessageId => completedMessageId;

        public Subscription(Guid id, string name, Topic topic, long completedMessageId,
            ISubscriptionPointersStorage subscriptionPointersStorage)
        {
            this.id = id;
            this.name = name;
            this.topic = topic;
            this.completedMessageId = completedMessageId;
            this.subscriptionPointersStorage = subscriptionPointersStorage;
        }

        internal void Complete(long messageId)
        {
            lock (sync)
            {
                if (messageId > completedMessageId)
                {
                    completedMessageId = messageId;
                    subscriptionPointersStorage.Write(id, completedMessageId);
                }
            }
        }

        internal Subscriber CreateSubscriber(Func<ReadOnlyMemory<Message>, CancellationToken, Task> push, SubscriberOptions subscriberOptions)
        {
            lock (sync)
            {
                if (subscriber != null)
                {
                    throw new SubscriptionManagementException($"Subscription {name} is already used");
                }

                subscriber = new Subscriber(this, push, completedMessageId, subscriberOptions);
                subscriber.StartPushLoop();
                return subscriber;
            }
        }

        internal void DeleteSubscriber()
        {
            lock (sync)
            {
                if (subscriber == null)
                {
                    return;
                }

                subscriber.StopPushLoop();
                subscriber = null;
            }
        }

        public void Dispose()
        {
            subscriber?.Dispose();
        }
    }
}
