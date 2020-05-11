using FastQueue.Server.Core.Abstractions;
using FastQueue.Server.Core.Model;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FastQueue.Server.Core
{
    internal class Topic
    {
        private InfiniteArray<Message> data;
        private HashSet<TopicWriter> writers;
        private long offset;
        private readonly IPersistentStorage persistentStorage;
        private long persistedOffset;
        private int confirmationIntervalMilliseconds;
        private object dataSync = new object();
        private object writersSync = new object();

        public Topic(long initialOffset, IPersistentStorage persistentStorage, TopicOptions topicOptions)
        {
            offset = initialOffset;
            persistedOffset = initialOffset;
            this.persistentStorage = persistentStorage;
            confirmationIntervalMilliseconds = topicOptions.ConfirmationIntervalMilliseconds;
            data = new InfiniteArray<Message>(initialOffset, topicOptions.DataArrayOptions);
            writers = new HashSet<TopicWriter>();
        }

        public TopicWriteResult Write(ReadOnlySpan<ReadOnlyMemory<byte>> messages)
        {
            lock (dataSync)
            {
                var enqueuedTime = DateTime.UtcNow;
                var newMessages = new Message[messages.Length];
                for (int i = 0; i < newMessages.Length; i++)
                {
                    newMessages[i] = new Message(offset + i, enqueuedTime, messages[i]);
                }

                var ind = data.Add(newMessages);
                persistentStorage.Write(newMessages.AsSpan());
                offset += messages.Length;
                return new TopicWriteResult(ind, enqueuedTime);
            }
        }

        public TopicWriteResult Write(ReadOnlyMemory<byte> message)
        {
            lock (dataSync)
            {
                var enqueuedTime = DateTime.UtcNow;
                var newMessage = new Message(offset, enqueuedTime, message);
                var ind = data.Add(newMessage);
                persistentStorage.Write(newMessage);
                offset++;
                return new TopicWriteResult(ind, enqueuedTime);
            }
        }

        public void FreeTo(long offset)
        {
            lock (dataSync)
            {
                data.FreeTo(offset);
                persistentStorage.FreeTo(offset);
            }
        }

        public TopicWriter CreateWriter(Func<PublisherAck, Task> ackHandler)
        {
            lock (writersSync)
            {
                var writer = new TopicWriter(this, ackHandler);
                writers.Add(writer);
                return writer;
            }
        }

        public void DeleteWriter(TopicWriter writer)
        {
            lock (writersSync)
            {
                writers.Remove(writer);
            }
        }

        public async Task ConfirmationLoop(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                bool offsetChanged;
                lock (dataSync)
                {
                    offsetChanged = persistedOffset != offset;
                    if (offsetChanged)
                    {
                        persistentStorage.Flush();
                        persistedOffset = offset;
                    }
                }

                if (offsetChanged)
                {
                    TaskHelper.FireAndForget(FireAcks);
                }

                await Task.Delay(confirmationIntervalMilliseconds);
            }
        }

        private void FireAcks()
        {
            TopicWriter[] writersArr;
            int len;
            lock (writersSync)
            {
                len = writers.Count;
                if (len == 0)
                {
                    return;
                }

                writersArr = ArrayPool<TopicWriter>.Shared.Rent(len);
                writers.CopyTo(writersArr, 0, len);
            }

            try
            {
                for (int i = 0; i < len; i++)
                {
                    // sending most recent persistedOffset
                    writersArr[i].SendAck(persistedOffset);
                }
            }
            finally
            {
                ArrayPool<TopicWriter>.Shared.Return(writersArr);
            }
        }
    }

    public class TopicOptions
    {
        public int ConfirmationIntervalMilliseconds { get; set; } = 50;
        public InfiniteArrayOptions DataArrayOptions { get; set; } = new InfiniteArrayOptions();

        public TopicOptions()
        {
        }

        public TopicOptions(TopicOptions options)
        {
            ConfirmationIntervalMilliseconds = options.ConfirmationIntervalMilliseconds;
            DataArrayOptions = new InfiniteArrayOptions(options.DataArrayOptions);
        }
    }
}
