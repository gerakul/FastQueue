using System;
using System.Collections.Generic;
using System.Text;

namespace FastQueue.Client
{
    public class SubscriberOptions
    {
        public int PushIntervalMilliseconds { get; set; } = 50;
        public int MaxMessagesInBatch { get; set; } = 10000;

        public SubscriberOptions()
        {
        }

        public SubscriberOptions(SubscriberOptions options)
        {
            PushIntervalMilliseconds = options.PushIntervalMilliseconds;
            MaxMessagesInBatch = options.MaxMessagesInBatch;
        }
    }
}
