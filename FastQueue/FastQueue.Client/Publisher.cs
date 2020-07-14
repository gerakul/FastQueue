using FastQueue.Client.Abstractions;
using FastQueue.Client.Exceptions;
using FastQueueService;
using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FastQueue.Client
{
    internal class Publisher : IPublisher
    {
        private readonly AsyncDuplexStreamingCall<WriteRequest, PublisherAck> duplexStream;
        private readonly Action<long> ackHandler;
        private readonly IClientStreamWriter<WriteRequest> requestStream;
        private readonly IAsyncStreamReader<PublisherAck> responseStream;
        private CancellationTokenSource cancellationTokenSource;
        private long sequenceNumber;
        private object sync = new object();
        private Task<Task> ackLoopTask;
        private bool disposed = false;

        internal Publisher(Grpc.Core.AsyncDuplexStreamingCall<FastQueueService.WriteRequest, FastQueueService.PublisherAck> duplexStream,
            Action<long> ackHandler)
        {
            this.duplexStream = duplexStream;
            this.ackHandler = ackHandler;
            requestStream = duplexStream.RequestStream;
            responseStream = duplexStream.ResponseStream;
            cancellationTokenSource = new CancellationTokenSource();
            sequenceNumber = 1;
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

                writeTask = requestStream.WriteAsync(new WriteRequest 
                { 
                    SequenceNumber = sequenceNumber, 
                    Message = Google.Protobuf.ByteString.CopyFrom(message.Span) 
                });

                seqNum = sequenceNumber++;
            }

            await writeTask;
            return seqNum;
        }

        internal void StartAckLoop()
        {
            ackLoopTask = Task.Factory.StartNew(() => AckLoop(cancellationTokenSource.Token), TaskCreationOptions.LongRunning);
        }

        private async Task AckLoop(CancellationToken cancellationToken)
        {
            try
            {
                await foreach (var ack in responseStream.ReadAllAsync(cancellationToken))
                {
                    ackHandler?.Invoke(ack.SequenceNumber);
                }
            }
            catch (TaskCanceledException)
            {
            }
            catch (Grpc.Core.RpcException ex) when (ex.StatusCode == StatusCode.Cancelled)
            {
            }
            catch
            {
                TaskHelper.FireAndForget(async () => await DisposeAsync());
            }
        }

        public async ValueTask DisposeAsync()
        {
            lock (sync)
            {
                if (disposed)
                {
                    return;
                }

                disposed = true;
            }

            cancellationTokenSource.Cancel();
            await await ackLoopTask;

            await requestStream.CompleteAsync();

            duplexStream?.Dispose();
        }
    }
}
