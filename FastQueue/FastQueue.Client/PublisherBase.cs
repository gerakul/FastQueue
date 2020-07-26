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
        private readonly Func<long, Task> ackHandlerAsync;
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
            Initialize();
        }

        internal PublisherBase(IAsyncStreamReader<PublisherAck> responseStream, Func<long, Task> ackHandler)
        {
            this.responseStream = responseStream;
            this.ackHandlerAsync = ackHandler;
            Initialize();
        }

        private void Initialize()
        {
            cancellationTokenSource = new CancellationTokenSource();
            sequenceNumber = 1;
        }

        internal void StartAckLoop()
        {
            if (ackHandler == null && ackHandlerAsync == null)
            {
                ackLoopTask = null;
            }
            else
            {
                ackLoopTask = Task.Factory.StartNew(() => AckLoop(cancellationTokenSource.Token), TaskCreationOptions.LongRunning);
            }
        }

        private async Task AckLoop(CancellationToken cancellationToken)
        {
            try
            {
                if (ackHandlerAsync != null)
                {
                    await foreach (var ack in responseStream.ReadAllAsync(cancellationToken))
                    {
                        await ackHandlerAsync(ack.SequenceNumber);
                    }
                }
                else
                {
                    await foreach (var ack in responseStream.ReadAllAsync(cancellationToken))
                    {
                        ackHandler(ack.SequenceNumber);
                    }
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
            if (ackLoopTask != null)
            {
                await await ackLoopTask;
            }

            return true;
        }

        public abstract ValueTask DisposeAsync();
    }
}
