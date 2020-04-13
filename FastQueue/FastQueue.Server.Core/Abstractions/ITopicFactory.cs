using FastQueue.Server.Core.Model;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace FastQueue.Server.Core.Abstractions
{
    public interface ITopicFactory
    {
        ITopic CreateTopic(string topicName, TopicOptions options);
    }
}
