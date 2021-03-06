﻿using FastQueue.Client.Abstractions;
using FastQueue.Client.Exceptions;
using FastQueueService;
using Grpc.Core;
using System;
using System.Threading.Tasks;

namespace FastQueue.Client
{
    internal class Publisher : PublisherBase, IPublisher
    {
        private readonly AsyncDuplexStreamingCall<PublishRequest, PublisherAck> duplexStream;
        private readonly IClientStreamWriter<PublishRequest> requestStream;

        internal Publisher(Grpc.Core.AsyncDuplexStreamingCall<FastQueueService.PublishRequest, FastQueueService.PublisherAck> duplexStream,
            Action<long> ackHandler) : base(duplexStream.ResponseStream, ackHandler)
        {
            this.duplexStream = duplexStream;
            requestStream = duplexStream.RequestStream;
        }

        internal Publisher(Grpc.Core.AsyncDuplexStreamingCall<FastQueueService.PublishRequest, FastQueueService.PublisherAck> duplexStream,
            Func<long, Task> ackHandler) : base(duplexStream.ResponseStream, ackHandler)
        {
            this.duplexStream = duplexStream;
            requestStream = duplexStream.RequestStream;
        }

        public async Task<long> Publish(ReadOnlyMemory<byte> message)
        {
            Task writeTask;
            long seqNum;
            lock (sync)
            {
                if (disposed)
                {
                    throw new PublisherException($"Cannot write to disposed {nameof(Publisher)}");
                }

                writeTask = requestStream.WriteAsync(new PublishRequest
                { 
                    SequenceNumber = sequenceNumber, 
                    Message = Google.Protobuf.ByteString.CopyFrom(message.Span) 
                });

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
