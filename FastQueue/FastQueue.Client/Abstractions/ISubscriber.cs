using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace FastQueue.Client.Abstractions
{
    public interface ISubscriber : IAsyncDisposable
    {
        Task Complete(long messageId);
    }
}
