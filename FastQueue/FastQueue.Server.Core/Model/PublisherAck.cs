using System;
using System.Collections.Generic;
using System.Text;

namespace FastQueue.Server.Core.Model
{
    public readonly struct PublisherAck
    {
        public readonly long SequenceNumber;

        public PublisherAck(long sequenceNumber)
        {
            SequenceNumber = sequenceNumber;
        }
    }
}
