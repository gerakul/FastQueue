using System;
using System.Collections.Generic;
using System.Text;

namespace FastQueue.Server.Core.Model
{
    public readonly struct Message
    {
        public readonly long ID;
        public readonly DateTimeOffset EnqueuedTime;
        public readonly ISpan Body;

        public Message(long id, DateTimeOffset enqueuedTime, ISpan body)
        {
            ID = id;
            EnqueuedTime = enqueuedTime;
            Body = body;
        }
    }
}
