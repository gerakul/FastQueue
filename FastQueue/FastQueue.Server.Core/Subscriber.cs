using FastQueue.Server.Core.Exceptions;
using FastQueue.Server.Core.Model;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace FastQueue.Server.Core
{
    public class Subscriber : IDisposable
    {
        private const int PushIntervalMilliseconds = 50;

        private readonly Subscription subscription;
        private readonly Topic topic;
        private readonly Func<ReadOnlyMemory<Message>, CancellationToken, Task> pushHandler;

        private long sentMessageId;
        private CancellationTokenSource cancellationTokenSource;
        private object sync = new object();
        private bool disposed = false;

        internal Subscriber(Subscription subscription, Func<ReadOnlyMemory<Message>, CancellationToken, Task> pushHandler, long completedMessageId)
        {
            this.subscription = subscription;
            this.topic = subscription.Topic;
            this.pushHandler = pushHandler;
            sentMessageId = completedMessageId;
            cancellationTokenSource = new CancellationTokenSource();
        }

        internal void StartPushLoop()
        {
            Task.Factory.StartNew(async () => await PushLoop(cancellationTokenSource.Token), TaskCreationOptions.LongRunning);
        }

        internal void StopPushLoop()
        {
            cancellationTokenSource.Cancel();
        }

        private async Task PushLoop(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var persistedMessageId = topic.PersistedMessageId;
                    if (persistedMessageId > sentMessageId)
                    {
                        var dataSnapshot = topic.CurrentData;
                        await Push(dataSnapshot.Data, dataSnapshot.StartMessageId, cancellationToken);
                    }

                    await Task.Delay(PushIntervalMilliseconds, cancellationToken);
                }
                catch (TaskCanceledException)
                {
                    break;
                }
            }
        }

        private async Task Push(ReadOnlyMemory<Message>[] data, long startMessageId, CancellationToken cancellationToken)
        {
            try
            {
                int blockInd = 0;
                long blockStartMessageId = startMessageId;
                bool dataSent = false;
                long sentCount = 0;
                while (blockInd < data.Length && !cancellationToken.IsCancellationRequested)
                {
                    var blockLen = data[blockInd].Length;

                    if (sentMessageId < blockStartMessageId + blockLen - 1)
                    {
                        var memory = data[blockInd].Slice(checked((int)(sentMessageId - blockStartMessageId)) + 1);
                        await RunPushHandler(memory, cancellationToken);
                        sentCount = memory.Length;
                        dataSent = true;
                        blockInd++;
                        break;
                    }

                    blockInd++;
                    blockStartMessageId += blockLen;
                }

                while (blockInd < data.Length && !cancellationToken.IsCancellationRequested)
                {
                    await RunPushHandler(data[blockInd], cancellationToken);
                    sentCount += data[blockInd].Length;
                    blockInd++;
                }

                if (dataSent)
                {
                    long calculatedSentId = sentMessageId + sentCount;
                    sentMessageId = data[^1].Span[^1].ID;
                    // should never happen
                    if (sentMessageId != calculatedSentId)
                    {
                        throw new FatalException($"Offset doesn't match ID");
                    }
                }
            }
            catch
            {
                Dispose();
                throw;
            }
        }

        private Task RunPushHandler(ReadOnlyMemory<Message> messages, CancellationToken cancellationToken)
        {
            return pushHandler(messages, cancellationToken);
        }

        public void Complete(long messageId)
        {
            lock (sync)
            {
                if (disposed)
                {
                    throw new TopicWriterException($"Cannot complete message to disposed {nameof(Subscriber)}");
                }

                subscription.Complete(messageId);
            }
        }

        public void Dispose()
        {
            lock (sync)
            {
                if (disposed)
                {
                    return;
                }

                subscription.DeleteSubscriber();
                disposed = true;
            }
        }
    }
}
