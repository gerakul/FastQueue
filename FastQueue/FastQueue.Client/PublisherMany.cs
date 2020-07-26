using FastQueue.Client.Abstractions;
using FastQueue.Client.Exceptions;
using FastQueueService;
using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace FastQueue.Client
{
    internal class PublisherMany : PublisherBase, IPublisherMany
    {
        private readonly AsyncDuplexStreamingCall<WriteManyRequest, PublisherAck> duplexStream;
        private readonly IClientStreamWriter<WriteManyRequest> requestStream;

        internal PublisherMany(Grpc.Core.AsyncDuplexStreamingCall<FastQueueService.WriteManyRequest, FastQueueService.PublisherAck> duplexStream,
            Action<long> ackHandler) : base(duplexStream.ResponseStream, ackHandler)
        {
            this.duplexStream = duplexStream;
            requestStream = duplexStream.RequestStream;
        }

        internal PublisherMany(Grpc.Core.AsyncDuplexStreamingCall<FastQueueService.WriteManyRequest, FastQueueService.PublisherAck> duplexStream,
            Func<long, Task> ackHandler) : base(duplexStream.ResponseStream, ackHandler)
        {
            this.duplexStream = duplexStream;
            requestStream = duplexStream.RequestStream;
        }

        public async Task<long> Publish(IEnumerable<ReadOnlyMemory<byte>> messages)
        {
            Task writeTask;
            long seqNum;
            lock (sync)
            {
                if (disposed)
                {
                    throw new PublisherException($"Cannot write to disposed {nameof(Publisher)}");
                }

                var request = new WriteManyRequest
                {
                    SequenceNumber = sequenceNumber
                };

                request.Messages.AddRange(messages.Select(x => Google.Protobuf.ByteString.CopyFrom(x.Span)));

                writeTask = requestStream.WriteAsync(request);

                seqNum = sequenceNumber++;
            }

            await writeTask;
            return seqNum;
        }

        public override async ValueTask DisposeAsync()
        {
            if (await DisposeBase())
            {
                await requestStream.CompleteAsync();
                duplexStream?.Dispose();
            }
        }
    }
}
