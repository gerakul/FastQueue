using FastQueue.Server.Core.Abstractions;
using FastQueue.Server.Core.Model;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace FastQueue.Server.Core
{
    public abstract class TopicBase : ITopic
    {
        private HashSet<IAckSender> ackSenders = new HashSet<IAckSender>();

        public string Name { get; }

        internal TopicBase(string name)
        {
            Name = name;
        }

        public abstract void Write(PublisherMessage message);

        public abstract void Write(PublisherMessage[] messages);

        public void SetAckSender(IAckSender ackSender)
        {
            ackSenders.Add(ackSender);
        }

        private Task SendAcks()
        {
            throw new NotImplementedException();
        }
    }
}
