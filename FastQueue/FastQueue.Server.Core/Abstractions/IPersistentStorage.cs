using FastQueue.Server.Core.Model;
using System;
using System.Collections.Generic;
using System.Text;

namespace FastQueue.Server.Core.Abstractions
{
    public interface IPersistentStorage : IDisposable
    {
        void Write(ReadOnlySpan<Message> messages);
        void Write(Message message);
        void FreeTo(long firstValidMessageId);
        void Flush();
    }
}
