using System;
using System.Collections.Generic;
using System.Text;

namespace FastQueue.Server.Core.Abstractions
{
    public class SubscriptionsConfiguration
    {
        public List<SubscriptionConfiguration> Subscriptions { get; set; }
    }

    public class SubscriptionConfiguration
    {
        public Guid Id { get; set; }
        public string Name { get; set; }
    }
}
