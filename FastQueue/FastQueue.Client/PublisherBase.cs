using FastQueueService;
using Grpc.Core;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace FastQueue.Client
{
    internal abstract class PublisherBase : IAsyncDisposable
    {
        private readonly Action<long> ackHandler;
        private readonly IAsyncStreamReader<PublisherAck> responseStream;
        private Task<Task> ackLoopTask;
        private CancellationTokenSource cancellationTokenSource;

        protected long sequenceNumber;
        protected object sync = new object();
        protected bool disposed = false;

        internal PublisherBase(IAsyncStreamReader<PublisherAck> responseStream, Action<long> ackHandler)
        {
            this.responseStream = responseStream;
            this.ackHandler = ackHandler;
            cancellationTokenSource = new CancellationTokenSource();
            sequenceNumber = 1;
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
                throw;
            }
        }

        protected async ValueTask<bool> DisposeBase()
        {
            lock (sync)
            {
                if (disposed)
                {
                    return false;
                }

                disposed = true;
            }

            cancellationTokenSource.Cancel();
            await await ackLoopTask;
            return true;
        }

        public abstract ValueTask DisposeAsync();
    }
}
