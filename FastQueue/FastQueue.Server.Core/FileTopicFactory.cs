using FastQueue.Server.Core.Abstractions;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace FastQueue.Server.Core
{
    public class FileTopicFactory : ITopicFactory
    {
        private readonly string directoryPath;
        private readonly TopicOptions topicOptions;
        private readonly long persistentStorageFileLengthThreshold;
        private readonly long subscriptionPointersStorageFileLengthThreshold;

        public FileTopicFactory(TopicFactoryOptions options)
        {
            directoryPath = options.DirectoryPath;
            topicOptions = options.TopicOptions;
            persistentStorageFileLengthThreshold = options.PersistentStorageFileLengthThreshold;
            subscriptionPointersStorageFileLengthThreshold = options.SubscriptionPointersStorageFileLengthThreshold;
        }

        public ITopicManagement CreateTopic(string name)
        {
            var persistentStorage = new FilePersistentStorage(new FilePersistentStorageOptions
            {
                DirectoryPath = Path.Combine(directoryPath, name),
                FileLengthThreshold = persistentStorageFileLengthThreshold
            });

            var subscriptionsConfigurationStorage = new SubscriptionsConfigurationFileStorage(new SubscriptionsConfigurationFileStorageOptions
            {
                DirectoryPath = Path.Combine(directoryPath, name)
            });

            var subscriptionPointersStorage = new SubscriptionPointersFileStorage(new SubscriptionPointersFileStorageOptions
            { 
                DirectoryPath = Path.Combine(directoryPath, name),
                FileLengthThreshold = subscriptionPointersStorageFileLengthThreshold
            });

            return new Topic(name, persistentStorage, subscriptionsConfigurationStorage, subscriptionPointersStorage, topicOptions);
        }
    }

    public class TopicFactoryOptions
    {
        public string DirectoryPath { get; set; }
        public TopicOptions TopicOptions { get; set; }
        public long PersistentStorageFileLengthThreshold { get; set; } = 100 * 1024 * 1024;
        public long SubscriptionPointersStorageFileLengthThreshold { get; set; } = 10 * 1024 * 1024;


        public TopicFactoryOptions()
        {
        }

        public TopicFactoryOptions(TopicFactoryOptions options)
        {
            DirectoryPath = options.DirectoryPath;
            TopicOptions = options.TopicOptions;
            PersistentStorageFileLengthThreshold = options.PersistentStorageFileLengthThreshold;
            SubscriptionPointersStorageFileLengthThreshold = options.SubscriptionPointersStorageFileLengthThreshold;
        }
    }
}
