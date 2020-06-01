using FastQueue.Server.Core.Abstractions;
using FastQueue.Server.Core.Model;
using System;
using System.IO;
using System.Text;

namespace FastQueue.Server.Core
{
    internal class FilePersistentStorage : IPersistentStorage
    {
        private class FileDescriptor
        {
            public long StartMessageId;
            public string Name;
            public FileDescriptor Next;
        }

        private long fileLengthThreshold;
        private string namePrefix;
        private string directoryPath;
        private BinaryWriter writer;
        private FileStream fileStream;
        private FileDescriptor firstFile;
        private FileDescriptor currentFile;
        private long currentLength;
        private long secondFileStartMessageId;

        public FilePersistentStorage(FilePersistentStorageOptions options)
        {
            fileLengthThreshold = options.FileLengthThreshold;
            namePrefix = options.NamePrefix;
            directoryPath = options.DirectoryPath;
        }

        public void Restore()
        {
            // ::: restoring state from drive will be here
            firstFile = currentFile = StartNewFile(0L);
            secondFileStartMessageId = long.MaxValue;
        }

        public void Write(ReadOnlySpan<Message> messages)
        {
            CheckForNewFile(messages[0].ID);

            for (int i = 0; i < messages.Length; i++)
            {
                WriteMessage(messages[i]);
            }
        }

        public void Write(Message message)
        {
            CheckForNewFile(message.ID);
            WriteMessage(message);
        }

        public void FreeTo(long firstValidMessageId)
        {
            if (secondFileStartMessageId > firstValidMessageId)
            {
                return;
            }

            var file = firstFile;
            var next = firstFile.Next;
            while (next != null)
            {
                if (next.StartMessageId > firstValidMessageId)
                {
                    firstFile = file;
                    secondFileStartMessageId = file.Next?.StartMessageId ?? long.MaxValue;
                    return;
                }

                File.Delete(file.Name);
                file.Next = null;
                file = next;
                next = next.Next;
            }

            firstFile = file;
            secondFileStartMessageId = file.Next?.StartMessageId ?? long.MaxValue;
        }

        public void Flush()
        {
            fileStream.Flush(true);
        }

        public void Dispose()
        {
            Flush();
            writer.Dispose();
            fileStream.Dispose();
        }

        private void WriteMessage(Message message)
        {
            writer.Write(message.ID);
            writer.Write(message.EnqueuedTime.ToBinary());
            writer.Write(message.Body.Length);
            writer.Write(message.Body.Span);

            currentLength += message.Body.Length + 20;
        }

        private void CheckForNewFile(long idToWrite)
        {
            if (currentLength < fileLengthThreshold)
            {
                return;
            }

            Flush();
            writer.Dispose();
            fileStream.Dispose();

            currentFile.Next = StartNewFile(idToWrite);
            currentFile = currentFile.Next;
            secondFileStartMessageId = firstFile.Next?.StartMessageId ?? long.MaxValue;
        }

        private FileDescriptor StartNewFile(long startMessageId)
        {
            var name = GetFileName(startMessageId);
            fileStream = File.OpenWrite(name);
            writer = new BinaryWriter(fileStream, Encoding.UTF8, true);
            currentLength = 0;
            return new FileDescriptor
            {
                Name = name,
                StartMessageId = startMessageId
            };
        }

        private string GetFileName(long startMessageId)
        {
            return Path.Combine(directoryPath, $"{namePrefix}_{startMessageId}.bin");
        }
    }

    public class FilePersistentStorageOptions
    {
        public long FileLengthThreshold { get; set; } = 1024 * 1024 * 1024;
        public string NamePrefix { get; set; } = "Data";
        public string DirectoryPath { get; set; }

        public FilePersistentStorageOptions()
        {
        }

        public FilePersistentStorageOptions(FilePersistentStorageOptions options)
        {
            FileLengthThreshold = options.FileLengthThreshold;
            NamePrefix = options.NamePrefix;
            DirectoryPath = options.DirectoryPath;
        }
    }

}
