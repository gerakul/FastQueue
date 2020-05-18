using FastQueue.Server.Core.Exceptions;
using FastQueue.Server.Core.Model;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Text;

namespace FastQueue.Server.Core
{
    internal class Subscription
    {
        private long offset;
        private Subscriber subscriber;

        internal void Push(ReadOnlyMemory<Message>[] data, int blockLength)
        {
            var sb = subscriber;
            if (sb == null)
            {
                return;
            }

            if (data.Length == 0 || data[0].Length == 0
                || offset - data[^1].Span[^1].ID == 1)
            {
                return;
            }

            int blockIndex;
            long secondBlockStartID;
            if (data.Length == 1 || (secondBlockStartID = data[1].Span[0].ID) > offset)
            {
                blockIndex = 0;
            }
            else
            {
                
                blockIndex = checked((int)((offset - secondBlockStartID) / blockLength) + 1);
            }

            int indexInBlock = checked((int)(offset - data[blockIndex].Span[0].ID));

            if (data[blockIndex].Span[indexInBlock].ID != offset)
            {
                throw new FatalException($"Offset doesn't match ID");
            }

            TaskHelper.FireAndForget(async () => await sb.Push(data, blockIndex, indexInBlock));
        }
    }
}
