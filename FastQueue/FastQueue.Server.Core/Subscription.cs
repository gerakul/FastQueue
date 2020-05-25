using FastQueue.Server.Core.Exceptions;
using FastQueue.Server.Core.Model;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FastQueue.Server.Core
{
    internal class Subscription
    {
        private long completedMessageId;
        private Subscriber subscriber;
        private int running;

        internal async Task Push(ReadOnlyMemory<Message>[] data, long startMessageId)
        {
            try
            {
                if (Interlocked.CompareExchange(ref running, 1, 0) == 1)
                {
                    return;
                }

                if (subscriber == null)
                {
                    return;
                }

                await subscriber.Push(data, startMessageId);
            }
            finally
            {
                Interlocked.Exchange(ref running, 0);
            }
        }


    }
}
