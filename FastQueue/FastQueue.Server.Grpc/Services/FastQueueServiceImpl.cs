using FastQueue.Server.Core.Abstractions;
using FastQueueService;
using Grpc.Core;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace FastQueue.Server.Grpc.Services
{
    public class FastQueueServiceImpl : FastQueueService.FastQueueService.FastQueueServiceBase
    {
        private readonly IServer server;

        public FastQueueServiceImpl(IServer server)
        {
            this.server = server;
        }

        public override Task<FastQueueService.CreateTopicReply> CreateTopic(FastQueueService.CreateTopicRequest request, ServerCallContext context)
        {
            server.CreateNewTopic(request.Name);
            return Task.FromResult(new FastQueueService.CreateTopicReply());
        }

        public override async Task<DeleteTopicReply> DeleteTopic(DeleteTopicRequest request, ServerCallContext context)
        {
            await server.DeleteTopic(request.Name, request.DeleteSubscriptions);
            return new FastQueueService.DeleteTopicReply();
        }

        public override Task<CreateSubscriptionReply> CreateSubscription(CreateSubscriptionRequest request, ServerCallContext context)
        {
            var topic = server.GetTopic(request.TopicName);
            topic.CreateSubscription(request.SubscriptionName);
            return Task.FromResult(new FastQueueService.CreateSubscriptionReply());
        }

        public override async Task<DeleteSubscriptionReply> DeleteSubscription(DeleteSubscriptionRequest request, ServerCallContext context)
        {
            var topic = server.GetTopic(request.TopicName);
            await topic.DeleteSubscription(request.SubscriptionName);
            return new FastQueueService.DeleteSubscriptionReply();
        }

        public override async Task Publish(IAsyncStreamReader<FastQueueService.WriteRequest> requestStream, IServerStreamWriter<FastQueueService.PublisherAck> responseStream, ServerCallContext context)
        {
            if (!(await requestStream.MoveNext(context.CancellationToken)))
            {
                return;
            }

            var opt = requestStream.Current.Options;
            var topic = server.GetTopic(opt.TopicName);
            await using var writer = topic.CreateWriter(opt.AckHandlerIsNull ? (Func<Core.Model.PublisherAck, CancellationToken, Task>)null 
                : (ack, ct) => responseStream.WriteAsync(new FastQueueService.PublisherAck { SequenceNumber = ack.SequenceNumber }),
                new Core.TopicWriterOptions
                {
                    ConfirmationIntervalMilliseconds = opt.ConfirmationIntervalMilliseconds
                });

            while (await requestStream.MoveNext(context.CancellationToken))
            {
                // ::: change ToByteArray on Memory when available
                writer.Write(new Core.Model.WriteRequest(requestStream.Current.SequenceNumber, requestStream.Current.Message.ToByteArray()));
            }
        }

        public override async Task PublishMany(IAsyncStreamReader<FastQueueService.WriteManyRequest> requestStream, IServerStreamWriter<FastQueueService.PublisherAck> responseStream, ServerCallContext context)
        {
            if (!(await requestStream.MoveNext(context.CancellationToken)))
            {
                return;
            }

            var opt = requestStream.Current.Options;
            var topic = server.GetTopic(opt.TopicName);
            await using var writer = topic.CreateWriter(opt.AckHandlerIsNull ? (Func<Core.Model.PublisherAck, CancellationToken, Task>)null
                : (ack, ct) => responseStream.WriteAsync(new FastQueueService.PublisherAck { SequenceNumber = ack.SequenceNumber }),
                new Core.TopicWriterOptions
                {
                    ConfirmationIntervalMilliseconds = opt.ConfirmationIntervalMilliseconds
                });

            while (await requestStream.MoveNext(context.CancellationToken))
            {
                // ::: change ToByteArray on Memory when available
                writer.Write(new Core.Model.WriteManyRequest(requestStream.Current.SequenceNumber, requestStream.Current.Messages.Select(x => new ReadOnlyMemory<byte>(x.ToByteArray())).ToArray()));
            }
        }

        public override async Task Subscribe(IAsyncStreamReader<FastQueueService.CompleteRequest> requestStream, IServerStreamWriter<FastQueueService.MessageBatch> responseStream, ServerCallContext context)
        {
            if (!(await requestStream.MoveNext(context.CancellationToken)))
            {
                return;
            }

            var opt = requestStream.Current.Options;
            var topic = server.GetTopic(opt.TopicName);
            await using var subscriber = topic.Subscribe(opt.SubscriptionName, (ms, ct) => responseStream.WriteAsync(CreateMessages(ms.Span)),
                new Core.SubscriberOptions
                {
                    MaxMessagesInBatch  = opt.MaxMessagesInBatch,
                    PushIntervalMilliseconds = opt.PushIntervalMilliseconds
                });

            while (await requestStream.MoveNext(context.CancellationToken))
            {
                subscriber.Complete(requestStream.Current.Id);
            }
        }

        private FastQueueService.MessageBatch CreateMessages(ReadOnlySpan<Core.Model.Message> messages)
        {
            var messagesToSend = new FastQueueService.MessageBatch();

            for (int i = 0; i < messages.Length; i++)
            {
                messagesToSend.Messages.Add(new FastQueueService.Message
                {
                    Id = messages[i].ID,
                    Timestamp = messages[i].EnqueuedTime.Ticks,
                    Body = Google.Protobuf.ByteString.CopyFrom(messages[i].Body.Span)
                });
            }

            return messagesToSend;
        }
    }
}
