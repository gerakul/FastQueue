using FastQueue.Client.Abstractions;
using FastQueue.Client.Exceptions;
using FastQueueService;
using Grpc.Core;
using Grpc.Net.Client;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace FastQueue.Client
{
    public class FastQueueClient : IFastQueueClient
    {
        private readonly GrpcChannel channel;
        private readonly FastQueueService.FastQueueService.FastQueueServiceClient grpcClient;

        public FastQueueClient(FastQueueClientOptions options)
        {
            channel = GrpcChannel.ForAddress(options.ServerUrl);
            grpcClient = new FastQueueService.FastQueueService.FastQueueServiceClient(channel);
        }

        public async Task CreateTopic(string name, CancellationToken cancellationToken)
        {
            await grpcClient.CreateTopicAsync(new FastQueueService.CreateTopicRequest { Name = name }, cancellationToken: cancellationToken);
        }

        public Task<IPublisher> CreatePublisher(string topicName, Action<long> ackHandler, PublisherOptions options = null)
        {
            return CreatePublisherInternal(topicName, options, duplexStream => new Publisher(duplexStream, ackHandler));
        }

        public Task<IPublisher> CreatePublisher(string topicName, Func<long, Task> ackHandler, PublisherOptions options = null)
        {
            return CreatePublisherInternal(topicName, options, duplexStream => new Publisher(duplexStream, ackHandler));
        }

        private async Task<IPublisher> CreatePublisherInternal(string topicName, PublisherOptions options, 
            Func<AsyncDuplexStreamingCall<PublishRequest, PublisherAck>, Publisher> publisherFactory)
        {
            var opt = options ?? new PublisherOptions();
            var duplexStream = grpcClient.Publish();
            await duplexStream.RequestStream.WriteAsync(new FastQueueService.PublishRequest
            {
                Options = new FastQueueService.PublisherOptions
                {
                    TopicName = topicName,
                    ConfirmationIntervalMilliseconds = opt.ConfirmationIntervalMilliseconds
                }
            });
            var publisher = publisherFactory(duplexStream);
            publisher.StartAckLoop();
            return publisher;
        }

        public Task<IPublisherMany> CreatePublisherMany(string topicName, Action<long> ackHandler, PublisherOptions options = null)
        {
            return CreatePublisherManyInternal(topicName, options, ackHandler == null, duplexStream => new PublisherMany(duplexStream, ackHandler));
        }

        public Task<IPublisherMany> CreatePublisherMany(string topicName, Func<long, Task> ackHandler, PublisherOptions options = null)
        {
            return CreatePublisherManyInternal(topicName, options, ackHandler == null, duplexStream => new PublisherMany(duplexStream, ackHandler));
        }

        private async Task<IPublisherMany> CreatePublisherManyInternal(string topicName, PublisherOptions options, bool ackHandlerIsNull,
            Func<AsyncDuplexStreamingCall<PublishManyRequest, PublisherAck>, PublisherMany> publisherFactory)
        {
            var opt = options ?? new PublisherOptions();
            var duplexStream = grpcClient.PublishMany();
            await duplexStream.RequestStream.WriteAsync(new FastQueueService.PublishManyRequest
            {
                Options = new FastQueueService.PublisherOptions
                {
                    TopicName = topicName,
                    ConfirmationIntervalMilliseconds = opt.ConfirmationIntervalMilliseconds,
                    AckHandlerIsNull = ackHandlerIsNull
                }
            });
            var publisher = publisherFactory(duplexStream);
            publisher.StartAckLoop();
            return publisher;
        }

        public Task<ISubscriber> CreateSubscriber(string topicName, string subscriptionName, Action<ISubscriber, IEnumerable<Message>> messagesHandler,
            SubscriberOptions options = null)
        {
            if (messagesHandler == null)
            {
                throw new ArgumentNullException(nameof(messagesHandler));
            }

            return CreateSubscriberInternal(topicName, subscriptionName, options, duplexStream => new Subscriber(duplexStream, messagesHandler));
        }

        public Task<ISubscriber> CreateSubscriber(string topicName, string subscriptionName, Func<ISubscriber, IEnumerable<Message>, Task> messagesHandler,
            SubscriberOptions options = null)
        {
            if (messagesHandler == null)
            {
                throw new ArgumentNullException(nameof(messagesHandler));
            }

            return CreateSubscriberInternal(topicName, subscriptionName, options, duplexStream => new Subscriber(duplexStream, messagesHandler));
        }

        private async Task<ISubscriber> CreateSubscriberInternal(string topicName, string subscriptionName, SubscriberOptions options,
            Func<AsyncDuplexStreamingCall<FastQueueService.CompleteRequest, FastQueueService.MessageBatch>, Subscriber> subscriberFactory)
        {
            var opt = options ?? new SubscriberOptions();
            var duplexStream = grpcClient.Subscribe();
            await duplexStream.RequestStream.WriteAsync(new FastQueueService.CompleteRequest
            {
                Options = new FastQueueService.SubscriberOptions
                {
                    TopicName = topicName,
                    SubscriptionName = subscriptionName,
                    MaxMessagesInBatch = opt.MaxMessagesInBatch,
                    PushIntervalMilliseconds = opt.PushIntervalMilliseconds
                }
            });
            var subscriber = subscriberFactory(duplexStream);
            subscriber.StartReceivingLoop();
            return subscriber;
        }

        public void Dispose()
        {
            channel?.Dispose();
        }
    }
}
