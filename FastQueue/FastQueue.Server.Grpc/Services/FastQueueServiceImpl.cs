using FastQueue.Server.Core.Abstractions;
using FastQueueService;
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

        public override Task<CreateTopicReply> CreateTopic(CreateTopicRequest request, ServerCallContext context)
        {
            server.CreateNewTopic(request.Name);
            return Task.FromResult(new CreateTopicReply());
        }

        public override async Task Publish(IAsyncStreamReader<WriteRequest> requestStream, IServerStreamWriter<PublisherAck> responseStream, ServerCallContext context)
        {
            if (!(await requestStream.MoveNext(context.CancellationToken)))
            {
                return;
            }

            var topic = server.GetTopic(requestStream.Current.TopicName);
            await using var writer = topic.CreateWriter((ack, ct) => responseStream.WriteAsync(new PublisherAck { SequenceNumber = ack.SequenceNumber }),
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

        public override async Task PublishMany(IAsyncStreamReader<WriteManyRequest> requestStream, IServerStreamWriter<PublisherAck> responseStream, ServerCallContext context)
        {
            if (!(await requestStream.MoveNext(context.CancellationToken)))
            {
                return;
            }

            var topic = server.GetTopic(requestStream.Current.TopicName);
            await using var writer = topic.CreateWriter((ack, ct) => responseStream.WriteAsync(new PublisherAck { SequenceNumber = ack.SequenceNumber }),
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
    }
}
