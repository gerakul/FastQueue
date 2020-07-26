using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FastQueue.Client.Abstractions
{
    public interface IFastQueueClient : IDisposable
    {
        Task CreateTopic(string name, CancellationToken cancellationToken);
        Task<IPublisher> CreatePublisher(string topicName, Action<long> ackHandler, PublisherOptions options = null);
        Task<IPublisher> CreatePublisher(string topicName, Func<long, Task> ackHandler, PublisherOptions options = null);
        Task<IPublisherMany> CreatePublisherMany(string topicName, Action<long> ackHandler, PublisherOptions options = null);
        Task<IPublisherMany> CreatePublisherMany(string topicName, Func<long, Task> ackHandler, PublisherOptions options = null);
        Task<ISubscriber> CreateSubscriber(string topicName, string subscriptionName, Action<ISubscriber, IEnumerable<Message>> messagesHandler,
            SubscriberOptions options = null);
        Task<ISubscriber> CreateSubscriber(string topicName, string subscriptionName, Func<ISubscriber, IEnumerable<Message>, Task> messagesHandler,
            SubscriberOptions options = null);
    }
}
