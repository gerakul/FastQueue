using FastQueue.Server.Core.Abstractions;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;

namespace FastQueue.Server.Core
{
    internal class SubscriptionsConfigurationFileStorage : ISubscriptionsConfigurationStorage
    {
        private const string ConfigFileName = "SubscriptionsConfig";
        private const string MapFileName = "SubscriptionsMap";

        private string directoryPath;
        private string configFile;
        private string mapFile;
        private string configFileNew;
        private string mapFileNew;

        internal SubscriptionsConfigurationFileStorage(SubscriptionsConfigurationFileStorageOptions options)
        {
            directoryPath = options.DirectoryPath;
            configFile = Path.Combine(directoryPath, ConfigFileName + ".json");
            mapFile = Path.Combine(directoryPath, MapFileName + ".bin");
            configFileNew = Path.Combine(directoryPath, ConfigFileName + "_new.json");
            mapFileNew = Path.Combine(directoryPath, MapFileName + "_new.bin");
        }

        public void Update(SubscriptionsConfiguration subscriptionsConfiguration)
        {
            if (File.Exists(configFileNew))
            {
                File.Delete(configFileNew);
            }

            if (File.Exists(mapFileNew))
            {
                File.Delete(mapFileNew);
            }

            CreateNewSubscriptionsConfigurationFile(subscriptionsConfiguration);
            CreateNewSubscriptionsMapFile(subscriptionsConfiguration);

            if (File.Exists(configFile))
            {
                File.Delete(configFile);
            }

            if (File.Exists(mapFile))
            {
                File.Delete(mapFile);
            }

            File.Move(configFileNew, configFile);
            File.Move(mapFileNew, mapFile);
        }

        public SubscriptionsConfiguration Read()
        {
            var config = new SubscriptionsConfiguration
            {
                Subscriptions = new List<SubscriptionConfiguration>()
            };

            if (!File.Exists(mapFile))
            {
                return config;
            }

            using (var reader = new BinaryReader(File.OpenRead(mapFile), Encoding.UTF8, false))
            {
                SubscriptionConfiguration subscriptionConfiguration; 
                while ((subscriptionConfiguration = ReadMapRecord(reader)) != null)
                {
                    config.Subscriptions.Add(subscriptionConfiguration);
                }
            }

            return config;
        }

        private void CreateNewSubscriptionsConfigurationFile(SubscriptionsConfiguration subscriptionsConfiguration)
        {
            var intenralConfig = new SubscriptionsConfigurationInternal(subscriptionsConfiguration);
            var text = JsonSerializer.Serialize(intenralConfig, new JsonSerializerOptions { WriteIndented = true });
            File.WriteAllText(configFileNew, text, Encoding.UTF8);
        }

        private void CreateNewSubscriptionsMapFile(SubscriptionsConfiguration subscriptionsConfiguration)
        {
            var subscriptions = subscriptionsConfiguration.Subscriptions ?? new List<SubscriptionConfiguration>();

            using (var writer = new BinaryWriter(File.OpenWrite(mapFileNew), Encoding.UTF8, false))
            {
                foreach (var item in subscriptions)
                {
                    WriteMapRecord(item, writer);
                }

                writer.Flush();
            }
        }

        private void WriteMapRecord(SubscriptionConfiguration subscriptionConfiguration, BinaryWriter writer)
        {
            writer.Write(subscriptionConfiguration.Id.ToByteArray());
            writer.Write(subscriptionConfiguration.Name);
        }

        private SubscriptionConfiguration ReadMapRecord(BinaryReader reader)
        {
            try
            {
                var idBytes = reader.ReadBytes(16);
                var name = reader.ReadString();

                return new SubscriptionConfiguration
                {
                    Id = new Guid(idBytes),
                    Name = name
                };
            }
            catch (EndOfStreamException)
            {
                return null;
            }
        }
    }

    public class SubscriptionsConfigurationFileStorageOptions
    {
        public string DirectoryPath { get; set; }

        public SubscriptionsConfigurationFileStorageOptions()
        {
        }

        public SubscriptionsConfigurationFileStorageOptions(SubscriptionsConfigurationFileStorageOptions options)
        {
            DirectoryPath = options.DirectoryPath;
        }
    }

    internal class SubscriptionsConfigurationInternal
    {
        public List<string> Subscriptions { get; set; }

        public SubscriptionsConfigurationInternal()
        {
        }

        public SubscriptionsConfigurationInternal(SubscriptionsConfiguration subscriptionsConfiguration)
        {
            if ((subscriptionsConfiguration.Subscriptions?.Count ?? 0) == 0)
            {
                Subscriptions = new List<string>();
            }

            Subscriptions = subscriptionsConfiguration.Subscriptions.Select(x => x.Name).ToList();
        }
    }
}
