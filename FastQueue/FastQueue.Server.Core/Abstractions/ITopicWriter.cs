using FastQueue.Server.Core.Model;
using System;
using System.Collections.Generic;
using System.Text;

namespace FastQueue.Server.Core.Abstractions
{
    public interface ITopicWriter : IAsyncDisposable
    {
        void Write(WriteManyRequest request);
        void Write(WriteRequest request);
    }
}
