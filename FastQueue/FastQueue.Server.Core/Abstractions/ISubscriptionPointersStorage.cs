using System;
using System.Collections.Generic;
using System.Text;

namespace FastQueue.Server.Core.Abstractions
{
    public interface ISubscriptionPointersStorage : IDisposable
    {
        void Write(Guid subscriptionId, long completedId);
        void Flush();
        Dictionary<Guid, long> Restore(Func<Dictionary<Guid, long>> currentPointersGetter);
    }
}
