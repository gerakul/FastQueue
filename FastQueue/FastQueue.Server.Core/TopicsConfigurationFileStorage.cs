using FastQueue.Server.Core.Abstractions;
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;

namespace FastQueue.Server.Core
{
    internal class TopicsConfigurationFileStorage : ITopicsConfigurationStorage
    {
        private readonly string configFile;
        private readonly string configFileNew;

        internal TopicsConfigurationFileStorage(TopicsConfigurationFileStorageOptions options)
        {
            configFile = options.ConfigurationFile;
            configFileNew = Path.Combine(Path.GetDirectoryName(configFile), 
                Path.GetFileNameWithoutExtension(configFile) + "_new" + Path.GetExtension(configFile));
        }

        public void Update(TopicsConfiguration topicsConfiguration)
        {
            CreateNewTopicsConfigurationFile(topicsConfiguration);

            if (File.Exists(configFile))
            {
                File.Delete(configFile);
            }

            File.Move(configFileNew, configFile);
        }

        public TopicsConfiguration Read()
        {
            if (!File.Exists(configFile))
            {
                return new TopicsConfiguration { Topics = new List<TopicConfiguration>() };
            }

            var text = File.ReadAllText(configFile, Encoding.UTF8);
            return JsonSerializer.Deserialize<TopicsConfiguration>(text);
        }

        private void CreateNewTopicsConfigurationFile(TopicsConfiguration topicsConfiguration)
        {
            var text = JsonSerializer.Serialize(topicsConfiguration, new JsonSerializerOptions { WriteIndented = true });
            File.WriteAllText(configFileNew, text, Encoding.UTF8);
        }
    }

    public class TopicsConfigurationFileStorageOptions
    {
        public string ConfigurationFile { get; set; }

        public TopicsConfigurationFileStorageOptions()
        {
        }

        public TopicsConfigurationFileStorageOptions(TopicsConfigurationFileStorageOptions options)
        {
            ConfigurationFile = options.ConfigurationFile;
        }
    }
}
