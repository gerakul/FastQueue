using FastQueue.Client.Abstractions;
using FastQueue.Client.Exceptions;
using FastQueueService;
using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FastQueue.Client
{
    internal class PublisherMany : IPublisherMany
    {
        private readonly AsyncDuplexStreamingCall<WriteManyRequest, PublisherAck> duplexStream;
        private readonly Action<long> ackHandler;
        private readonly IClientStreamWriter<WriteManyRequest> requestStream;
        private readonly IAsyncStreamReader<PublisherAck> responseStream;
        private CancellationTokenSource cancellationTokenSource;
        private long sequenceNumber;
        private object sync = new object();
        private Task<Task> ackLoopTask;
        private bool disposed = false;

        internal PublisherMany(Grpc.Core.AsyncDuplexStreamingCall<FastQueueService.WriteManyRequest, FastQueueService.PublisherAck> duplexStream,
            Action<long> ackHandler)
        {
            this.duplexStream = duplexStream;
            this.ackHandler = ackHandler;
            requestStream = duplexStream.RequestStream;
            responseStream = duplexStream.ResponseStream;
            cancellationTokenSource = new CancellationTokenSource();
            sequenceNumber = 1;
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
