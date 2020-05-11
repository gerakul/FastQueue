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
            public long StartOffset;
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
        private long secondFileStartOffset;

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
            secondFileStartOffset = long.MaxValue;
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

        public void FreeTo(long offset)
        {
            if (secondFileStartOffset > offset)
            {
                return;
            }

            var file = firstFile;
            var next = firstFile.Next;
            while (next != null)
            {
                if (next.StartOffset >= offset)
                {
                    firstFile = file;
                    secondFileStartOffset = file.Next?.StartOffset ?? long.MaxValue;
                    return;
                }

                File.Delete(file.Name);
                file.Next = null;
                file = next;
                next = next.Next;
            }

            firstFile = file;
            secondFileStartOffset = file.Next?.StartOffset ?? long.MaxValue;
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
            secondFileStartOffset = firstFile.Next?.StartOffset ?? long.MaxValue;
        }

        private FileDescriptor StartNewFile(long startOffset)
        {
            var name = GetFileName(startOffset);
            fileStream = File.OpenWrite(name);
            writer = new BinaryWriter(fileStream, Encoding.UTF8, true);
            currentLength = 0;
            return new FileDescriptor
            {
                Name = name,
                StartOffset = startOffset
            };
        }

        private string GetFileName(long startOffset)
        {
            return Path.Combine(directoryPath, $"{namePrefix}_{startOffset}.bin");
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
