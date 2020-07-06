using FastQueue.Server.Core.Abstractions;
using FastQueue.Server.Core.Exceptions;
using FastQueue.Server.Core.Model;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace FastQueue.Server.Core
{
    internal class TopicWriter : ITopicWriter
    {
        private const int ConfirmationIntervalMilliseconds = 50;

        private readonly struct IdPair
        {
            public readonly long WriterID;
            public readonly long ID;

            public IdPair(long writerID, long id)
            {
                WriterID = writerID;
                ID = id;
            }
        }

        private Topic topic;
        private Func<PublisherAck, CancellationToken, Task> ackHandler;
        private InfiniteArray<IdPair> idMap;
        private long lastAckedMessageId;
        private object sync = new object();
        private bool disposed = false;
        private CancellationTokenSource cancellationTokenSource;
        private Task<Task> confirmationLoopTask;

        internal TopicWriter(Topic topic, Func<PublisherAck, CancellationToken, Task> ackHandler)
        {
            this.topic = topic;
            this.ackHandler = ackHandler;
            lastAckedMessageId = -1;
            cancellationTokenSource = new CancellationTokenSource();
            idMap = new InfiniteArray<IdPair>(0, new InfiniteArrayOptions
            {
                MinimumFreeBlocks = 4,
                DataListCapacity = 128,
                BlockLength = 10000
            });
        }

        public void Write(WriteManyRequest request)
        {
            lock (sync)
            {
                if (disposed)
                {
                    throw new TopicWriterException($"Cannot write to disposed {nameof(TopicWriter)}");
                }

                var writeResult = topic.Write(request.Messages.Span);
                idMap.Add(new IdPair(request.SequenceNumber, writeResult.LastInsertedIndex));
            }
        }

        public void Write(WriteRequest request)
        {
            lock (sync)
            {
                if (disposed)
                {
                    throw new TopicWriterException($"Cannot write to disposed {nameof(TopicWriter)}");
                }

                var writeResult = topic.Write(request.Message);
                idMap.Add(new IdPair(request.SequenceNumber, writeResult.LastInsertedIndex));
            }
        }

        internal void StartConfirmationLoop()
        {
            confirmationLoopTask = Task.Factory.StartNew(() => ConfirmationLoop(cancellationTokenSource.Token), TaskCreationOptions.LongRunning);
        }

        internal async Task StopConfirmationLoop()
        {
            cancellationTokenSource.Cancel();
            await await confirmationLoopTask;
        }

        private async Task ConfirmationLoop(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var persistedMessageId = topic.PersistedMessageId;
                    if (persistedMessageId > lastAckedMessageId)
                    {
                        long idToAck = FindSequenceNumberToAck(persistedMessageId);
                        if (idToAck >= 0)
                        {
                            await RunAckHandler(new PublisherAck(idToAck), cancellationToken);
                        }

                        lastAckedMessageId = persistedMessageId;
                    }

                    await Task.Delay(ConfirmationIntervalMilliseconds, cancellationToken);
                }
                catch (TaskCanceledException)
                {
                    break;
                }
                catch
                {
                    // this is ackHandler responsibility to worry about error handling
                    TaskHelper.FireAndForget(async () => await DisposeAsync());
                    // ::: logging instead of throwing
                    throw;
                }
            }
        }

        private long FindSequenceNumberToAck(long persistedId)
        {
            lock (sync)
            {
                if (disposed)
                {
                    // do not throw exception here
                    return -1;
                }

                var blocks = idMap.GetDataBlocks();
                var ind = idMap.GetLastItemIndex();

                for (int i = blocks.Length - 1; i >= 0; i--)
                {
                    var m = blocks[i].Span;
                    for (int j = m.Length - 1; j >= 0; j--)
                    {
                        if (m[j].ID <= persistedId)
                        {
                            var idToAck = m[j].WriterID;
                            idMap.FreeTo(ind + 1);
                            return idToAck;
                        }

                        ind--;
                    }
                }
            }

            return -1;
        }

        private async Task RunAckHandler(PublisherAck ack, CancellationToken cancellationToken)
        {
            await ackHandler(ack, cancellationToken).ConfigureAwait(false);
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

            await topic.DeleteWriter(this);
            topic = null;
        }
    }
}
