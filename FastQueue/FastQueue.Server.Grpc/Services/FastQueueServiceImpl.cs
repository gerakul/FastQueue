using FastQueue.Server.Core.Abstractions;
using FastQueue.Server.Core.Model;
using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Linq;
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

        public override async Task Publish(IAsyncStreamReader<FastQueueService.WriteRequest> requestStream, IServerStreamWriter<FastQueueService.PublisherAck> responseStream, ServerCallContext context)
        {
            if (!(await requestStream.MoveNext(context.CancellationToken)))
            {
                return;
            }

            var topic = server.GetTopic(requestStream.Current.TopicName);
            await using var writer = topic.CreateWriter((ack, ct) => responseStream.WriteAsync(new FastQueueService.PublisherAck { SequenceNumber = ack.SequenceNumber }),
                new Core.TopicWriterOptions
                {
                    // ::: from config
                    ConfirmationIntervalMilliseconds = 100
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

            var topic = server.GetTopic(requestStream.Current.TopicName);
            await using var writer = topic.CreateWriter((ack, ct) => responseStream.WriteAsync(new FastQueueService.PublisherAck { SequenceNumber = ack.SequenceNumber }),
                new Core.TopicWriterOptions
                {
                    // ::: from config
                    ConfirmationIntervalMilliseconds = 100
                });

            while (await requestStream.MoveNext(context.CancellationToken))
            {
                // ::: change ToByteArray on Memory when available
                writer.Write(new Core.Model.WriteManyRequest(requestStream.Current.SequenceNumber, requestStream.Current.Messages.Select(x => new ReadOnlyMemory<byte>(x.ToByteArray())).ToArray()));
            }
        }

        public override async Task Subscribe(IAsyncStreamReader<FastQueueService.CompleteRequest> requestStream, IServerStreamWriter<FastQueueService.Messages> responseStream, ServerCallContext context)
        {
            if (!(await requestStream.MoveNext(context.CancellationToken)))
            {
                return;
            }

            var topic = server.GetTopic(requestStream.Current.TopicName);
            await using var subscriber = topic.Subscribe(requestStream.Current.SubscriptionName, (ms, ct) => responseStream.WriteAsync(CreateMessages(ms.Span)),
                new Core.SubscriberOptions
                {
                    // ::: from config
                    MaxMessagesInBatch  = 10000,
                    PushIntervalMilliseconds = 50
                });

            while (await requestStream.MoveNext(context.CancellationToken))
            {
                subscriber.Complete(requestStream.Current.Id);
            }
        }

        private FastQueueService.Messages CreateMessages(ReadOnlySpan<Message> messages)
        {
            var messagesToSend = new FastQueueService.Messages();

            for (int i = 0; i < messages.Length; i++)
            {
                messagesToSend.Messages_.Add(new FastQueueService.Message
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
