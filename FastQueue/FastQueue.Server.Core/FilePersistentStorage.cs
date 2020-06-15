using FastQueue.Server.Core.Abstractions;
using FastQueue.Server.Core.Exceptions;
using FastQueue.Server.Core.Model;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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

        public IEnumerable<Message> Restore()
        {
            var candidates = Directory.GetFiles(directoryPath, $"{namePrefix}_*.bin", SearchOption.TopDirectoryOnly);
            var preLen = namePrefix.Length + 1;

            List<FileDescriptor> files = new List<FileDescriptor>();
            foreach (var item in candidates)
            {
                var idString = Path.GetFileNameWithoutExtension(item).Substring(preLen);
                if (long.TryParse(idString, out var id))
                {
                    files.Add(new FileDescriptor { Name = item, StartMessageId = id });
                }
            }

            if (files.Count == 0)
            {
                firstFile = currentFile = StartNewFile(0L);
                secondFileStartMessageId = long.MaxValue;
                yield break;
            }

            files.Sort(new Comparison<FileDescriptor>((a, b) => a.StartMessageId.CompareTo(b.StartMessageId)));
            firstFile = files[0];
            Message lastMessage = new Message(-1, DateTime.MinValue, null);
            foreach (var m in ReadFile(firstFile, true, lastMessage))
            {
                yield return m;
                lastMessage = m;
            }

            if (lastMessage.ID == -1 && files.Count > 1)
            {
                throw new RestoreException($"First file was empty but second file found {files[1].Name}");
            }

            for (int i = 1; i < files.Count; i++)
            {
                files[i - 1].Next = files[i];
                foreach (var m in ReadFile(files[i], false, lastMessage))
                {
                    yield return m;
                    lastMessage = m;
                }
            }

            secondFileStartMessageId = firstFile.Next == null ? long.MaxValue : firstFile.Next.StartMessageId;
            currentFile = files[^1];
            fileStream = File.OpenWrite(currentFile.Name);
            writer = new BinaryWriter(fileStream, Encoding.UTF8, true);
            currentLength = fileStream.Length;
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

        private Message ReadMessage(BinaryReader reader)
        {
            long id;
            try
            {
                id = reader.ReadInt64();
            }
            catch (EndOfStreamException)
            {
                return new Message(-1, DateTime.MinValue, null);
            }

            var time = DateTime.FromBinary(reader.ReadInt64());
            var len = reader.ReadInt32();
            var body = reader.ReadBytes(len);

            return new Message(id, time, body);
        }

        private IEnumerable<Message> ReadFile(FileDescriptor fileDescriptor, bool isFirst, Message lastMessage)
        {
            using (var reader = new BinaryReader(File.OpenRead(fileDescriptor.Name), Encoding.UTF8, false))
            {
                Message m = ReadMessage(reader);
                if (m.ID == -1)
                {
                    if (!isFirst)
                    {
                        throw new RestoreException($"Only first file can be empty");
                    }

                    if (fileDescriptor.StartMessageId != 0)
                    {
                        throw new RestoreException($"Empty first file should have a StartMessageId = 0. Actual value is {fileDescriptor.StartMessageId}");
                    }

                    yield break;
                }

                if (fileDescriptor.StartMessageId != m.ID)
                {
                    throw new RestoreException($"File name {Path.GetFileName(fileDescriptor.Name)} doesn't match the first message {m.ID}");
                }

                if (!isFirst)
                {
                    MessageOrderCheck(lastMessage, m, fileDescriptor.Name);
                }

                var last = m;
                while ((m = ReadMessage(reader)).ID != -1)
                {
                    MessageOrderCheck(last, m, fileDescriptor.Name);
                    yield return m;
                    last = m;
                }
            }
        }

        private void MessageOrderCheck(Message lastMessage, Message message, string fileName)
        {
            if (message.ID - lastMessage.ID != 1)
            {
                throw new RestoreException($"Missing messages in the file {fileName}. Last {lastMessage.ID}, current {message.ID}");
            }

            if (message.EnqueuedTime < lastMessage.EnqueuedTime)
            {
                throw new RestoreException($"Wrong EnqueuedTime for the message {message.ID} in the file {fileName}. Last {lastMessage.EnqueuedTime:yyyy-MM-dd HH:mm:ss.fffffff}, current {message.EnqueuedTime:yyyy-MM-dd HH:mm:ss.fffffff}");
            }
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
