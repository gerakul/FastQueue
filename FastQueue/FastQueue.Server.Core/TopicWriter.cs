using FastQueue.Server.Core.Abstractions;
using FastQueue.Server.Core.Model;
using System;
using System.Collections.Generic;
using System.Text;

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
        private InfiniteArray<IdPair> idMap;
        private object sync = new object();
        private bool disposed = false;

        internal TopicWriter(Topic topic)
        {
            this.topic = topic;
            idMap = new InfiniteArray<IdPair>(0, new InfiniteArrayOptions
            {
                MinimumFreeBlocks = 4,
                DataListCapacity = 128,
                BlockLength = 10000
            });
        }

        public void Write(WriteManyRequest request)
        {
            if (disposed)
            {
                return; // throw exception?
            }

            var writeResult = topic.Write(request.Messages.Span);
            idMap.Add(new IdPair(request.SequenceNumber, writeResult.LastInsertedIndex));
        }

        public void Write(WriteRequest request)
        {
            if (disposed)
            {
                return; // throw exception?
            }

            var writeResult = topic.Write(request.Message);
            idMap.Add(new IdPair(request.SequenceNumber, writeResult.LastInsertedIndex));
        }

        internal void SendAck(long persistedId)
        {
            if (disposed)
            {
                return; // throw exception?
            }
        }

        ~TopicWriter()
        {
            Dispose();
        }

        public void Dispose()
        {
            if (disposed)
            {
                return;
            }

            disposed = true;
            topic.DeleteWriter(this);
            topic = null;
        }
    }
}
