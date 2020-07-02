using FastQueue.Server.Core.Abstractions;
using FastQueue.Server.Core.Exceptions;
using FastQueue.Server.Core.Model;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace FastQueue.Server.Core
{
    public class Subscriber : ISubscriber
    {
        private readonly Subscription subscription;
        private readonly Topic topic;
        private readonly Func<ReadOnlyMemory<Message>, CancellationToken, Task> pushHandler;
        private readonly int pushIntervalMilliseconds;
        private readonly int maxMessagesInBatch;

        private long sentMessageId;
        private CancellationTokenSource cancellationTokenSource;
        private object sync = new object();
        private bool disposing = false;
        private bool disposed = false;
        private Task<Task> pushLoopTask;


        internal Subscriber(Subscription subscription, Func<ReadOnlyMemory<Message>, CancellationToken, Task> pushHandler, long completedMessageId,
            SubscriberOptions subscriberOptions)
        {
            this.subscription = subscription;
            this.topic = subscription.Topic;
            this.pushHandler = pushHandler;
            sentMessageId = completedMessageId;
            maxMessagesInBatch = subscriberOptions.MaxMessagesInBatch;
            pushIntervalMilliseconds = subscriberOptions.PushIntervalMilliseconds;
            cancellationTokenSource = new CancellationTokenSource();
        }

        internal void StartPushLoop()
        {
            pushLoopTask = Task.Factory.StartNew(() => PushLoop(cancellationTokenSource.Token), TaskCreationOptions.LongRunning);
        }

        internal async Task StopPushLoop()
        {
            cancellationTokenSource.Cancel();
            await await pushLoopTask;
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

                        if (dataSnapshot != null)
                        {
                            await Push(dataSnapshot.Data, dataSnapshot.StartMessageId, cancellationToken);
                        }
                    }

                    await Task.Delay(pushIntervalMilliseconds, cancellationToken);
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
                        await SendBatches(memory, cancellationToken);
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
                    await SendBatches(data[blockInd], cancellationToken);
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
                await DisposeAsync();
                throw;
            }
        }

        private async Task SendBatches(ReadOnlyMemory<Message> messages, CancellationToken cancellationToken)
        {
            int ind = 0;
            int len = messages.Length - maxMessagesInBatch;

            while (ind < len)
            {
                await pushHandler(messages.Slice(ind, maxMessagesInBatch), cancellationToken);
                ind += maxMessagesInBatch;
            }

            if (ind < messages.Length)
            {
                await pushHandler(messages[ind..], cancellationToken);
            }
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

        public async ValueTask DisposeAsync()
        {
            lock (sync)
            {
                if (disposing)
                {
                    return;
                }

                disposing = true;
            }

            await subscription.DeleteSubscriber();

            lock (sync)
            {
                disposed = true;
            }
        }
    }

    public class SubscriberOptions
    {
        public int PushIntervalMilliseconds { get; set; } = 50;
        public int MaxMessagesInBatch { get; set; } = 10000;

        public SubscriberOptions()
        {
        }

        public SubscriberOptions(SubscriberOptions options)
        {
            PushIntervalMilliseconds = options.PushIntervalMilliseconds;
            MaxMessagesInBatch = options.MaxMessagesInBatch;
        }
    }
}
