using FastQueue.Server.Core.Exceptions;
using FastQueue.Server.Core.Model;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace FastQueue.Server.Core
{
    internal class Subscription
    {
        private long completedMessageId;
        private string name;
        private Subscriber subscriber;
        private int running;
        private object sync = new object();

        public Subscription(string name)
        {
            this.name = name;
        }

        internal async Task Push(ReadOnlyMemory<Message>[] data, long startMessageId)
        {
            try
            {
                if (Interlocked.CompareExchange(ref running, 1, 0) == 1)
                {
                    return;
                }

                if (subscriber == null)
                {
                    return;
                }

                await subscriber.Push(data, startMessageId);
            }
            catch
            {
                DeleteSubscriber();
            }
            finally
            {
                Interlocked.Exchange(ref running, 0);
            }
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

        public Subscriber CreateSubscriber(Func<ReadOnlyMemory<Message>, CancellationToken, Task> push)
        {
            lock (sync)
            {
                if (subscriber != null)
                {
                    throw new SubscriptionManagementException($"Subscription {name} is already used");
                }

                subscriber = new Subscriber(this, push, completedMessageId);
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

                subscriber = null;
            }
        }
    }
}
