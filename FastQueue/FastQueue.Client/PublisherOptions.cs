using System;
using System.Collections.Generic;
using System.Text;

namespace FastQueue.Client
{
    public class PublisherOptions
    {
        public int ConfirmationIntervalMilliseconds { get; set; } = 50;

        public PublisherOptions()
        {
        }

        public PublisherOptions(PublisherOptions options)
        {
            ConfirmationIntervalMilliseconds = options.ConfirmationIntervalMilliseconds;
        }
    }
}
