using FastQueue.Server.Core.Abstractions;
using FastQueue.Server.Core.Exceptions;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace FastQueue.Server.Core
{
    public class Server
    {
        private ITopicFactory topicFactory;
        private ConcurrentDictionary<string, ITopic> topics;

        public Server(ITopicFactory topicFactory)
        {
            this.topicFactory = topicFactory;
            topics = new ConcurrentDictionary<string, ITopic>();
        }

        public Task CreateTopic(string topicName, TopicOptions options = null)
        {
            topics.AddOrUpdate(topicName, name => topicFactory.CreateTopic(name, options ?? new TopicOptions()), 
                (name, y) => throw new TopicManagementException($"Topic {name} already exists"));

            return Task.CompletedTask;
        }

        public ITopic GetTopicWriter(string topicName)
        {
            if (topics.TryGetValue(topicName, out ITopic topic))
            {
                return topic;
            }

            throw new TopicManagementException($"Topic {topicName} doesn't exist");
        }
    }
}
