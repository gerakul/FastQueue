using FastQueue.Server.Core.Model;
using System;
using System.Collections.Generic;
using System.Text;

namespace FastQueue.Server.Core
{
    public class InMemoryTopic : TopicBase
    {
        internal InMemoryTopic(string name) : base(name)
        {
        }

        public override void Write(PublisherMessage message)
        {
            throw new NotImplementedException();
        }

        public override void Write(PublisherMessage[] messages)
        {
            throw new NotImplementedException();
        }
    }
}
