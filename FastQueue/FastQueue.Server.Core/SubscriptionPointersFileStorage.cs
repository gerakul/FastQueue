using FastQueue.Server.Core.Abstractions;
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;

namespace FastQueue.Server.Core
{
    internal class SubscriptionPointersFileStorage : ISubscriptionPointersStorage
    {
        private long fileLengthThreshold;
        private string namePrefix;
        private string directoryPath;
        private string fileName;
        private BinaryWriter writer;
        private FileStream fileStream;
        private long currentLength;
        private object sync = new object();
        private Func<Dictionary<Guid, long>> currentPointersGetter;

        internal SubscriptionPointersFileStorage(SubscriptionPointersFileStorageOptions options)
        {
            fileLengthThreshold = options.FileLengthThreshold;
            namePrefix = options.NamePrefix;
            directoryPath = options.DirectoryPath;
            fileName = Path.Combine(directoryPath, namePrefix + ".bin");
        }

        public void Write(Guid subscriptionId, long completedId)
        {
            lock (sync)
            {
                InternalWrite(writer, subscriptionId, completedId);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void InternalWrite(BinaryWriter binaryWriter, Guid subscriptionId, long completedId)
        {
            binaryWriter.Write(subscriptionId.ToByteArray());
            binaryWriter.Write(completedId);
            currentLength += 24;
        }

        private bool InternalRead(BinaryReader reader, out Guid subscriptionId, out long completedId)
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

                if (currentLength >= fileLengthThreshold)
                {
                    Recreate();
                }
            }
        }

        public void Dispose()
        {
            Flush();
            writer.Dispose();
            fileStream.Dispose();
        }

        public Dictionary<Guid, long> Restore(Func<Dictionary<Guid, long>> currentPointersGetter)
        {
            this.currentPointersGetter = currentPointersGetter;
            var dict = new Dictionary<Guid, long>();

            if (File.Exists(fileName))
            {
                Guid subscriptionId;
                long completedId;
                using (var reader = new BinaryReader(File.OpenRead(fileName), Encoding.UTF8, false))
                {
                    while (InternalRead(reader, out subscriptionId, out completedId))
                    {
                        dict[subscriptionId] = completedId;
                    }
                }

                fileStream = File.Open(fileName, FileMode.Append, FileAccess.Write, FileShare.Write);
            }
            else
            {
                fileStream = File.Create(fileName);
            }

            writer = new BinaryWriter(fileStream, Encoding.UTF8, true);
            currentLength = fileStream.Length;

            return dict;
        }

        private void Recreate()
        {
            var pointers = currentPointersGetter();
            writer.Dispose();
            fileStream.Dispose();

            var newFileName = Path.Combine(directoryPath, namePrefix + "_new.bin");

            using (var w = new BinaryWriter(File.Create(newFileName), Encoding.UTF8, false))
            {
                currentLength = 0;
                foreach (var item in pointers)
                {
                    InternalWrite(w, item.Key, item.Value);
                }
            }

            if (File.Exists(fileName))
            {
                File.Delete(fileName);
            }

            File.Move(newFileName, fileName);

            fileStream = File.Open(fileName, FileMode.Append, FileAccess.Write, FileShare.Write);
            writer = new BinaryWriter(fileStream, Encoding.UTF8, true);
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
