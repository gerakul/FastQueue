using System;
using System.Collections.Generic;
using System.Text;

namespace FastQueue.Server.Core.Abstractions
{
    public interface ISubscriptionsConfigurationStorage
    {
        void Update(SubscriptionsConfiguration subscriptionsConfiguration);
        SubscriptionsConfiguration Read();
    }
}
