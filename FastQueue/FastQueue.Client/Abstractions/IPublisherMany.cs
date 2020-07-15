using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace FastQueue.Client.Abstractions
{
    public interface IPublisherMany : IAsyncDisposable
    {
        Task<long> Publish(IEnumerable<ReadOnlyMemory<byte>> messages);
    }
}
