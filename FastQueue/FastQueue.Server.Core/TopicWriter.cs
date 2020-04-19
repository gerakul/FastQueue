using FastQueue.Server.Core.Abstractions;
using FastQueue.Server.Core.Model;
using System;
using System.Collections.Generic;
using System.Text;

namespace FastQueue.Server.Core
{
    internal class TopicWriter
    {
        private TopicBase topic;

        public TopicWriter(TopicBase topic)
        {
            this.topic = topic;
        }

        public void Write(PublisherMessage[] messages)
        {
            var lastId = topic.Write(messages);

        }

        public bool SendAck(long topicLastId)
        {
            return true;
        }
    }
}
