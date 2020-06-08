using System;

namespace FastQueue.Server.Core.Model
{
    public readonly struct Message
    {
        public readonly long ID;
        public readonly DateTime EnqueuedTime;
        public readonly ReadOnlyMemory<byte> Body;

        // ::: get rid of it
        internal readonly byte[] RestoredBody;

        public Message(long id, DateTime enqueuedTime, ReadOnlyMemory<byte> body)
        {
            ID = id;
            EnqueuedTime = enqueuedTime;
            Body = body;
            RestoredBody = null;
        }

        internal Message(long id, DateTime enqueuedTime, byte[] body)
        {
            ID = id;
            EnqueuedTime = enqueuedTime;
            Body = body;
            RestoredBody = body;
        }
    }
}
