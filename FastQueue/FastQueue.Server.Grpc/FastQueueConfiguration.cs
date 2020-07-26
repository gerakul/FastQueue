using FastQueue.Server.Core;
using Microsoft.Extensions.Configuration;

namespace FastQueue.Server.Grpc
{
    public class FastQueueConfiguration
    {
        private const string ConfigSection = "FastQueue";

        public TopicFactoryOptions TopicFactoryOptions { get; set; }
        public TopicsConfigurationFileStorageOptions TopicsConfigurationFileStorageOptions { get; set; }

        public static FastQueueConfiguration Read(IConfiguration configuration)
        {
            return configuration.GetSection(ConfigSection).Get<FastQueueConfiguration>();
        }
    }
}
