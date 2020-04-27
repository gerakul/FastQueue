using FastQueue.Server.Core.Abstractions;
using FastQueue.Server.Core.Model;
using System;
using System.Collections.Generic;
using System.Text;

namespace FastQueue.Server.Core
{
    public class TopicWriter
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

        public void Write(Span<PublisherMessage> messages)
        {
            var writeResult = topic.Write(messages);
            idMap.Add(new IdPair(messages[^1].SequenceNumber, writeResult.LastInsertedIndex));
        }

        internal bool SendAck(long persistedId)
        {
            return true;
        }
    }
}
