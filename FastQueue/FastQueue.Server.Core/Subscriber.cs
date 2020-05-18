using FastQueue.Server.Core.Model;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace FastQueue.Server.Core
{
    internal class Subscriber
    {
        private readonly Subscription subscription;
        private readonly ISubscriber push;

        public Subscriber(Subscription subscription, ISubscriber push)
        {
            this.subscription = subscription;
            this.push = push;
        }

        internal async Task Push(ReadOnlyMemory<Message>[] data, int blockIndex, int indexInBlock)
        {
            await push.Push(data[blockIndex].Span.Slice(indexInBlock));

            for (int i = blockIndex + 1; i < data.Length; i++)
            {
                await push.Push(data[i].Span);
            }
        }
    }

    public interface ISubscriber
    {
        Task Push(ReadOnlySpan<Message> messages);
    }


}
