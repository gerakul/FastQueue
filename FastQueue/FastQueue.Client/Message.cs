using System;
using System.Collections.Generic;
using System.Text;

namespace FastQueue.Client
{
    public readonly struct Message
    {
        public readonly long ID;
        public readonly DateTime EnqueuedTime;
        public readonly ReadOnlyMemory<byte> Body;

        // ::: Remove it when Memory will be available for ByteString in grpc
        internal readonly byte[] BodyOwner;

        public Message(long id, DateTime enqueuedTime, ReadOnlyMemory<byte> body)
        {
            ID = id;
            EnqueuedTime = enqueuedTime;
            Body = body;
            BodyOwner = null;
        }

        internal Message(long id, DateTime enqueuedTime, byte[] body)
        {
            ID = id;
            EnqueuedTime = enqueuedTime;
            Body = body;
            BodyOwner = body;
        }
    }
}
