using FastQueue.Server.Core.Abstractions;
using FastQueue.Server.Core.Model;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FastQueue.Server.Core
{
    public abstract class TopicBase : ITopic
    {
        private long lastMessageId = 0;

        public string Name { get; }

        internal TopicBase(string name)
        {
            Name = name;
        }

        public long Write(PublisherMessage message)
        {
            var newMessageId = Interlocked.Increment(ref lastMessageId);
            return newMessageId;
        }

        public long Write(PublisherMessage[] messages)
        {
            var newMessageId = Interlocked.Add(ref lastMessageId, messages.Length);
            return newMessageId;
        }

        private Task SendAcks()
        {
            throw new NotImplementedException();
        }
    }
}
