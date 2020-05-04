using System;

namespace FastQueue.Server.Core.Model
{
    public readonly struct WriteManyRequest
    {
        public readonly long SequenceNumber;
        public readonly ReadOnlyMemory<ReadOnlyMemory<byte>> Messages;

        public WriteManyRequest(long sequenceNumber, ReadOnlyMemory<byte>[] messages)
        {
            SequenceNumber = sequenceNumber;
            Messages = messages;
        }

        public WriteManyRequest(long sequenceNumber, ReadOnlyMemory<ReadOnlyMemory<byte>> messages)
        {
            SequenceNumber = sequenceNumber;
            Messages = messages;
        }
    }
}
