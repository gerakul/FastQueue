using FastQueue.Server.Core.Model;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FastQueue.Server.Core.Abstractions
{
    public interface ITopic
    {
        ITopicWriter CreateWriter(Func<PublisherAck, CancellationToken, Task> ackHandler, TopicWriterOptions topicWriterOptions);
        ISubscriber Subscribe(string subscriptionName, Func<ReadOnlyMemory<Message>, CancellationToken, Task> push,
            SubscriberOptions subscriberOptions);

        void CreateSubscription(string subscriptionName);
        void CreateSubscription(string subscriptionName, long startReadingFromId);
        Task DeleteSubscription(string subscriptionName);
        bool SubscriptionExists(string subscriptionName);
        string[] GetSubscriptions();
    }
}
