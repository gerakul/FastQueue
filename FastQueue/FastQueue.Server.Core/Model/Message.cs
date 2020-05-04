using System;

namespace FastQueue.Server.Core.Model
{
    public readonly struct Message
    {
        public readonly long ID;
        public readonly DateTimeOffset EnqueuedTime;
        public readonly ReadOnlyMemory<byte> Body;

        public Message(long id, DateTimeOffset enqueuedTime, ReadOnlyMemory<byte> body)
        {
            ID = id;
            EnqueuedTime = enqueuedTime;
            Body = body;
        }
    }
}
