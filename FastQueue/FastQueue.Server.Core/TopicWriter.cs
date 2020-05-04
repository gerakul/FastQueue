using FastQueue.Server.Core.Abstractions;
using FastQueue.Server.Core.Exceptions;
using FastQueue.Server.Core.Model;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace FastQueue.Server.Core
{
    public class TopicWriter : IDisposable
    {
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
        private Func<PublisherAck, Task> ackHandler;
        private InfiniteArray<IdPair> idMap;
        private object sync = new object();
        private bool disposed = false;

        internal TopicWriter(Topic topic, Func<PublisherAck, Task> ackHandler)
        {
            this.topic = topic;
            this.ackHandler = ackHandler;
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

        internal void SendAck(long persistedId)
        {
            long idToAck = FindSequenceNumberToAck(persistedId);
            if (idToAck >= 0)
            {
                TaskHelper.FireAndForget(async () => await RunAckHandler(new PublisherAck(idToAck)));
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

        private async Task RunAckHandler(PublisherAck ack)
        {
            try
            {
                await ackHandler(ack).ConfigureAwait(false);
            }
            catch
            {
                // this is ackHandler responsibility to worry about error handling
                // no matter what, dispose if we cannot
                Dispose();
                throw;
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

                topic.DeleteWriter(this);
                topic = null;
                disposed = true;
            }
        }
    }
}
