using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace FastQueue.Client.Abstractions
{
    public interface IPublisher : IAsyncDisposable
    {
        Task<long> Publish(ReadOnlyMemory<byte> message);
    }
}
