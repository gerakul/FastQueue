using System;
using System.Collections.Generic;
using System.Text;

namespace FastQueue.Server.Core
{
    internal readonly struct TopicWriteResult
    {
        public readonly long LastInsertedIndex;
        public readonly DateTime EnqueuedTime;

        public TopicWriteResult(long lastInsertedIndex, DateTime enqueuedTime)
        {
            LastInsertedIndex = lastInsertedIndex;
            EnqueuedTime = enqueuedTime;
        }
    }
}
