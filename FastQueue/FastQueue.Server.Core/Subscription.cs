using FastQueue.Server.Core.Exceptions;
using FastQueue.Server.Core.Model;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace FastQueue.Server.Core
{
    internal class Subscription : IDisposable
    {
        private long completedMessageId;
        private string name;
        private Topic topic;
        private Subscriber subscriber;
        private object sync = new object();

        internal Topic Topic => topic;
        internal long CompletedMessageId => completedMessageId;

        public Subscription(string name, Topic topic, long completedMessageId)
        {
            this.name = name;
            this.topic = topic;
            this.completedMessageId = completedMessageId;
        }

        internal void Complete(long messageId)
        {
            lock (sync)
            {
                if (messageId > completedMessageId)
                {
                    completedMessageId = messageId;
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

                subscriber.StartPushLoop();
                subscriber = null;
            }
        }

        public void Dispose()
        {
            subscriber?.Dispose();
        }
    }
}
