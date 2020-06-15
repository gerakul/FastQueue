using FastQueue.Server.Core.Abstractions;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace FastQueue.Server.Core
{
    public class SubscriptionPointersFileStorage : ISubscriptionPointersStorage
    {
        private long fileLengthThreshold;
        private string namePrefix;
        private string directoryPath;
        private BinaryWriter writer;
        private FileStream fileStream;
        private long currentLength;
        private object sync = new object();

        internal SubscriptionPointersFileStorage(SubscriptionPointersFileStorageOptions options)
        {
            fileLengthThreshold = options.FileLengthThreshold;
            namePrefix = options.NamePrefix;
            directoryPath = options.DirectoryPath;
        }

        public void Write(Guid subscriptionId, long completedId)
        {
            lock (sync)
            {
                writer.Write(subscriptionId.ToByteArray());
                writer.Write(completedId);
                currentLength += 24;
            }
        }

        private bool Read(BinaryReader reader, out Guid subscriptionId, out long completedId)
        {
            try
            {
                var idBytes = reader.ReadBytes(16);
                completedId = reader.ReadInt64();
                subscriptionId = new Guid(idBytes);
                return true;
            }
            catch (EndOfStreamException)
            {
                subscriptionId = default;
                completedId = default;
                return false;
            }
        }

        public void Flush()
        {
            lock (sync)
            {
                fileStream.Flush(true);
            }
        }

        public void Dispose()
        {
            Flush();
            writer.Dispose();
            fileStream.Dispose();
        }

        public Dictionary<Guid, long> Restore()
        {
            var dict = new Dictionary<Guid, long>();
            var fileName = Path.Combine(directoryPath, namePrefix + ".bin");

            if (File.Exists(fileName))
            {
                Guid subscriptionId;
                long completedId;
                using (var reader = new BinaryReader(File.OpenRead(fileName), Encoding.UTF8, false))
                {
                    while (Read(reader, out subscriptionId, out completedId))
                    {
                        dict[subscriptionId] = completedId;
                    }
                }

                fileStream = File.Open(fileName, FileMode.Append, FileAccess.Write, FileShare.Write);
            }
            else
            {
                fileStream = File.OpenWrite(fileName);
            }

            writer = new BinaryWriter(fileStream, Encoding.UTF8, true);
            currentLength = fileStream.Length;

            return dict;
        }
    }

    public class SubscriptionPointersFileStorageOptions
    {
        public long FileLengthThreshold { get; set; } = 10 * 1024 * 1024;
        public string NamePrefix { get; set; } = "SubscriptionPointers";
        public string DirectoryPath { get; set; }

        public SubscriptionPointersFileStorageOptions()
        {
        }

        public SubscriptionPointersFileStorageOptions(SubscriptionPointersFileStorageOptions options)
        {
            FileLengthThreshold = options.FileLengthThreshold;
            NamePrefix = options.NamePrefix;
            DirectoryPath = options.DirectoryPath;
        }

    }
}
