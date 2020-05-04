using System;

namespace FastQueue.Server.Core.Model
{
    public readonly struct WriteRequest
    {
        public readonly long SequenceNumber;
        public readonly ReadOnlyMemory<byte> Message;

        public WriteRequest(long sequenceNumber, ReadOnlyMemory<byte> message)
        {
            SequenceNumber = sequenceNumber;
            Message = message;
        }
    }
}
