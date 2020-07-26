using FastQueue.Client.Abstractions;
using FastQueue.Client.Exceptions;
using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FastQueue.Client
{
    public class Subscriber : ISubscriber
    {
        private readonly AsyncDuplexStreamingCall<FastQueueService.CompleteRequest, FastQueueService.MessageBatch> duplexStream;
        private readonly Action<ISubscriber, IEnumerable<Message>> messagesHandler;
        private readonly Func<ISubscriber, IEnumerable<Message>, Task> messagesHandlerAsync;
        private readonly IClientStreamWriter<FastQueueService.CompleteRequest> requestStream;
        private readonly IAsyncStreamReader<FastQueueService.MessageBatch> responseStream;
        private CancellationTokenSource cancellationTokenSource;
        private Task<Task> receivingLoopTask;
        private bool disposed = false;

        internal Subscriber(Grpc.Core.AsyncDuplexStreamingCall<FastQueueService.CompleteRequest, FastQueueService.MessageBatch> duplexStream,
            Action<ISubscriber, IEnumerable<Message>> messagesHandler)
        {
            this.duplexStream = duplexStream;
            this.messagesHandler = messagesHandler;
            requestStream = duplexStream.RequestStream;
            responseStream = duplexStream.ResponseStream;
            cancellationTokenSource = new CancellationTokenSource();
        }

        internal Subscriber(Grpc.Core.AsyncDuplexStreamingCall<FastQueueService.CompleteRequest, FastQueueService.MessageBatch> duplexStream,
            Func<ISubscriber, IEnumerable<Message>, Task> messagesHandlerAsync)
        {
            this.duplexStream = duplexStream;
            this.messagesHandlerAsync = messagesHandlerAsync;
            requestStream = duplexStream.RequestStream;
            responseStream = duplexStream.ResponseStream;
            cancellationTokenSource = new CancellationTokenSource();
        }

        internal void StartReceivingLoop()
        {
            receivingLoopTask = Task.Factory.StartNew(() => ReceivingLoop(cancellationTokenSource.Token), TaskCreationOptions.LongRunning);
        }

        private async Task ReceivingLoop(CancellationToken cancellationToken)
        {
            try
            {
                if (messagesHandlerAsync != null)
                {
                    await foreach (var messages in responseStream.ReadAllAsync(cancellationToken))
                    {
                        // ::: change ToByteArray on Memory when available
                        var receivedMessages = messages.Messages.Select(x => new Message(x.Id, new DateTime(x.Timestamp), new ReadOnlyMemory<byte>(x.Body.ToByteArray())));
                        await messagesHandlerAsync(this, receivedMessages);
                    }
                }
                else
                {
                    await foreach (var messages in responseStream.ReadAllAsync(cancellationToken))
                    {
                        // ::: change ToByteArray on Memory when available
                        var receivedMessages = messages.Messages.Select(x => new Message(x.Id, new DateTime(x.Timestamp), new ReadOnlyMemory<byte>(x.Body.ToByteArray())));
                        messagesHandler(this, receivedMessages);
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

        public async Task Complete(long messageId)
        {
            if (disposed)
            {
                throw new SubscriberException($"Cannot write to disposed {nameof(Subscriber)}");
            }

            await requestStream.WriteAsync(new FastQueueService.CompleteRequest
            {
                Id = messageId
            });
        }

        public async ValueTask DisposeAsync()
        {
            {
                if (disposed)
                {
                    return;
                }

                disposed = true;
            }

            cancellationTokenSource.Cancel();
            await await receivingLoopTask;

            await requestStream.CompleteAsync();

            duplexStream?.Dispose();
        }
    }
}
