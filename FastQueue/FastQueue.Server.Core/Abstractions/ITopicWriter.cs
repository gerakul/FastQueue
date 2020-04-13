using FastQueue.Server.Core.Model;
using System;
using System.Collections.Generic;
using System.Text;

namespace FastQueue.Server.Core.Abstractions
{
    public interface ITopicWriter
    {
        void Write(PublisherMessage message);

        void Write(PublisherMessage[] messages);

        void SetAckSender(IAckSender ackSender);
    }
}
