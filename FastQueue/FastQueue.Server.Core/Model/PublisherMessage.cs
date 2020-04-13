using System;
using System.Collections.Generic;
using System.Text;

namespace FastQueue.Server.Core.Model
{
    public class PublisherMessage
    {
        public int SequenceNumber { get; set; }
        public byte[] Message { get; set; }
    }
}
