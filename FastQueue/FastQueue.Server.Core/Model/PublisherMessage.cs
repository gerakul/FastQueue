using System;
using System.Collections.Generic;
using System.Text;

namespace FastQueue.Server.Core.Model
{
    public readonly struct PublisherMessage
    {
        public readonly long SequenceNumber;
        public readonly ISpan Body;

        public PublisherMessage(long sequenceNumber, ISpan body)
        {
            SequenceNumber = sequenceNumber;
            Body = body;
        }
    }
}
