using FastQueue.Server.Core.Model;
using System;
using System.Collections.Generic;
using System.Text;

namespace FastQueue.Server.Core
{
    internal class Topic
    {
        private InfiniteArray<Message> data;
        private HashSet<TopicWriter> writers;
        private long offset;
        private object dataSync = new object();
        private object writersSync = new object();

        public Topic(long initialOffset, TopicOptions topicOptions)
        {
            data = new InfiniteArray<Message>(initialOffset, topicOptions.DataArrayOptions);
            writers = new HashSet<TopicWriter>();
        }

        public void Write(Span<PublisherMessage> messages)
        {
            lock (dataSync)
            {
                var enqueuedTime = DateTimeOffset.UtcNow;
                var newMessages = new Message[messages.Length];
                for (int i = 0; i < messages.Length; i++)
                {
                    newMessages[i] = new Message(offset + i, enqueuedTime, messages[i].Body);
                }

                data.Add(newMessages);
                offset += messages.Length;
            }
        }

        public void Write(PublisherMessage message)
        {
            lock (dataSync)
            {
                data.Add(new Message(offset++, DateTimeOffset.UtcNow, message.Body));
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
    }

    public class TopicOptions
    {
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
