using System;
using System.Collections.Generic;
using System.Text;

namespace FastQueue.Server.Core.Model
{
    internal readonly struct TopicWriteResult
    {
        public readonly long LastInsertedIndex;
        public readonly DateTimeOffset EnqueuedTime;

        public TopicWriteResult(long lastInsertedIndex, DateTimeOffset enqueuedTime)
        {
            LastInsertedIndex = lastInsertedIndex;
            EnqueuedTime = enqueuedTime;
        }
    }
}
