using FastQueue.Server.Core.Abstractions;
using FastQueue.Server.Core.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FastQueue.Server.Core
{
    public class Server
    {
        private readonly ITopicFactory topicFactory;
        private readonly ITopicsConfigurationStorage topicsConfigurationStorage;
        private Dictionary<string, ITopicManagement> topics;
        private object sync = new object();
        private bool stopping = false;

        public Server(ITopicFactory topicFactory, ITopicsConfigurationStorage topicsConfigurationStorage)
        {
            this.topicFactory = topicFactory;
            this.topicsConfigurationStorage = topicsConfigurationStorage;
            topics = new Dictionary<string, ITopicManagement>();
        }

        public void Restore()
        {
            var config = topicsConfigurationStorage.Read();

            foreach (var item in config.Topics)
            {
                topics.Add(item.Name, topicFactory.CreateTopic(item.Name));
            }

            foreach (var item in topics.Values)
            {
                item.Restore();
            }

            foreach (var item in topics.Values)
            {
                item.Start();
            }
        }

        public async Task Stop()
        {
            List<ITopicManagement> topicsList;
            lock (sync)
            {
                if (stopping)
                {
                    return;
                }

                stopping = true;

                topicsList = topics.Values.ToList();
            }

            foreach (var item in topicsList)
            {
                await item.Stop(false);
            }
        }

        public void CreateNewTopic(string name)
        {
            ITopicManagement topic;
            lock (sync)
            {
                if (stopping)
                {
                    throw new TopicManagementException($"Cannot create topic when server is stopping");
                }

                if (topics.ContainsKey(name))
                {
                    throw new TopicManagementException($"Topic {name} already exists");
                }

                topic = topicFactory.CreateTopic(name);
                topics.Add(name, topic);

                UpdateTopicsConfiguration();
            }

            topic.Restore();
            topic.Start();
        }

        public async Task DeleteTopic(string name, bool deleteSubscriptions = false)
        {
            ITopicManagement topic;
            lock (sync)
            {
                if (stopping)
                {
                    throw new TopicManagementException($"Cannot delete topic when server is stopping");
                }

                topic = GetTopicInternal(name);
            }

            await topic.Stop(!deleteSubscriptions);
            topicFactory.DeleteTopic(topic);

            lock (sync)
            {
                topics.Remove(name);
                UpdateTopicsConfiguration();
            }
        }

        public ITopic GetTopic(string name)
        {
            lock (sync)
            {
                if (stopping)
                {
                    throw new TopicManagementException($"Cannot get topic when server is stopping");
                }

                return GetTopicInternal(name);
            }
        }

        private ITopicManagement GetTopicInternal(string name)
        {
            if (!topics.TryGetValue(name, out var topic))
            {
                throw new TopicManagementException($"Topic {name} does not exist");
            }

            return topic;
        }

        private void UpdateTopicsConfiguration()
        {
            var newConfig = new TopicsConfiguration
            {
                Topics = topics.Keys.Select(x => new TopicConfiguration
                {
                    Name = x
                }).ToList()
            };

            topicsConfigurationStorage.Update(newConfig);
        }
    }
}
