using FastQueue.Server.Core.Abstractions;
using FastQueue.Server.Core.Model;
using System;
using System.Collections.Generic;
using System.Text;

namespace FastQueue.Server.Core
{
    public class TopicFactory : ITopicFactory
    {
        public ITopic CreateTopic(string topicName, TopicOptions options)
        {
            //if (options.Persistent)
            //{
            //    throw new NotImplementedException();
            //}

            //return new InMemoryTopic(topicName);
            throw new NotImplementedException();
        }
    }
}
