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
        private long persistedOffset;
        private int confirmationIntervalMilliseconds;
        private object dataSync = new object();
        private object writersSync = new object();

        public Topic(long initialOffset, TopicOptions topicOptions)
        {
            confirmationIntervalMilliseconds = topicOptions.ConfirmationIntervalMilliseconds;
            offset = initialOffset;
            persistedOffset = initialOffset;
            data = new InfiniteArray<Message>(initialOffset, topicOptions.DataArrayOptions);
            writers = new HashSet<TopicWriter>();
        }

        public TopicWriteResult Write(Span<PublisherMessage> messages)
        {
            lock (dataSync)
            {
                var enqueuedTime = DateTimeOffset.UtcNow;
                var newMessages = new Message[messages.Length];
                for (int i = 0; i < messages.Length; i++)
                {
                    newMessages[i] = new Message(offset + i, enqueuedTime, messages[i].Body);
                }

                var ind = data.Add(newMessages);
                offset += messages.Length;
                return new TopicWriteResult(ind, enqueuedTime);
            }
        }

        public TopicWriteResult Write(PublisherMessage message)
        {
            lock (dataSync)
            {
                var enqueuedTime = DateTimeOffset.UtcNow;
                var ind = data.Add(new Message(offset, enqueuedTime, message.Body));
                offset++;
                return new TopicWriteResult(ind, enqueuedTime);
            }
        }

        public void FreeTo(long offset)
        {
            lock (dataSync)
            {
                data.FreeTo(offset);
            }
        }

        public TopicWriter CreateWriter()
        {
            lock (writersSync)
            {
                var writer = new TopicWriter(this);
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
                        // flush
                        persistedOffset = offset;
                    }
                }

                if (offsetChanged)
                {
                    TopicWriter[] writersArr;
                    int len;
                    lock (writersSync)
                    {
                        len = writers.Count;
                        if (len == 0)
                        {
                            goto delay;
                        }

                        writersArr = ArrayPool<TopicWriter>.Shared.Rent(len);
                        writers.CopyTo(writersArr, 0, len);
                    }

                    try
                    {
                        for (int i = 0; i < len; i++)
                        {
                            writersArr[i].SendAck(persistedOffset);
                        }
                    }
                    finally
                    {
                        ArrayPool<TopicWriter>.Shared.Return(writersArr);
                    }
                }

                delay: await Task.Delay(confirmationIntervalMilliseconds);
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
            DataArrayOptions = new InfiniteArrayOptions(options.DataArrayOptions);
        }
    }
}
