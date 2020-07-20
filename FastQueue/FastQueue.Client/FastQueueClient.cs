using FastQueue.Client.Abstractions;
using Grpc.Net.Client;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FastQueue.Client
{
    public class FastQueueClient : IDisposable
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

        public async Task<IPublisher> CreatePublisher(string topicName, Action<long> ackHandler)
        {
            var duplexStream = grpcClient.Publish();
            await duplexStream.RequestStream.WriteAsync(new FastQueueService.WriteRequest { TopicName = topicName });
            var publisher = new Publisher(duplexStream, ackHandler);
            publisher.StartAckLoop();
            return publisher;
        }

        public async Task<IPublisherMany> CreatePublisherMany(string topicName, Action<long> ackHandler)
        {
            var duplexStream = grpcClient.PublishMany();
            await duplexStream.RequestStream.WriteAsync(new FastQueueService.WriteManyRequest { TopicName = topicName });
            var publisher = new PublisherMany(duplexStream, ackHandler);
            publisher.StartAckLoop();
            return publisher;
        }

        public async Task<ISubscriber> CreateSubscriber(string topicName, string subscriptionName, Action<ISubscriber, IEnumerable<Message>> messagesHandler)
        {
            var duplexStream = grpcClient.Subscribe();
            await duplexStream.RequestStream.WriteAsync(new FastQueueService.CompleteRequest 
            { 
                TopicName = topicName,
                SubscriptionName = subscriptionName
            });
            var subscriber = new Subscriber(duplexStream, messagesHandler);
            subscriber.StartReceivingLoop();
            return subscriber;
        }

        public void Dispose()
        {
            channel?.Dispose();
        }
    }
}
