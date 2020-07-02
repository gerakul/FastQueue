using System;
using System.Collections.Generic;
using System.Text;

namespace FastQueue.Server.Core.Abstractions
{
    public interface ISubscriber : IAsyncDisposable
    {
        void Complete(long messageId);
    }
}
