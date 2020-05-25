using FastQueue.Server.Core.Exceptions;
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
        private readonly Func<ReadOnlyMemory<Message>, Task> push;

        private long sentMessageId;

        public Subscriber(Subscription subscription, Func<ReadOnlyMemory<Message>, Task> push, long completedMessageId)
        {
            this.subscription = subscription;
            this.push = push;
            sentMessageId = completedMessageId;
        }

        internal async Task Push(ReadOnlyMemory<Message>[] data, long startMessageId)
        {
            int blockInd = 0;
            long blockStartMessageId = startMessageId;
            bool dataSent = false;
            long sentCount = 0;
            while (blockInd < data.Length)
            {
                var blockLen = data[blockInd].Length;

                if (sentMessageId < blockStartMessageId + blockLen - 1)
                {
                    var memory = data[blockInd].Slice(checked((int)(sentMessageId - blockStartMessageId)) + 1);
                    await push(memory);
                    sentCount = memory.Length;
                    dataSent = true;
                    blockInd++;
                    break;
                }

                blockInd++;
                blockStartMessageId += blockLen;
            }

            while (blockInd < data.Length)
            {
                await push(data[blockInd]);
                sentCount = data[blockInd].Length;
                blockInd++;
            }

            if (dataSent)
            {
                long calculatedSentId = sentMessageId + sentCount;
                sentMessageId = data[^1].Span[^1].ID;
                // should never happen
                if (sentMessageId != calculatedSentId)
                {
                    throw new FatalException($"Offset doesn't match ID");
                }
            }
        }
    }
}
