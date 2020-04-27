using FastQueue.Server.Core.Abstractions;
using FastQueue.Server.Core.Model;
using System;
using System.Collections.Generic;
using System.Text;

namespace FastQueue.Server.Core
{
    public class TopicWriter
    {
        private Topic topic;

        internal TopicWriter(Topic topic)
        {
            this.topic = topic;
        }

        public void Write(Span<PublisherMessage> messages)
        {
            topic.Write(messages);

        }

        public bool SendAck(long topicLastId)
        {
            return true;
        }
    }
}
