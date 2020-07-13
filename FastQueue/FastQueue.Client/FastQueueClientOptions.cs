using System;
using System.Collections.Generic;
using System.Text;

namespace FastQueue.Client
{
    public class FastQueueClientOptions
    {
        public string ServerUrl { get; set; }

        public FastQueueClientOptions()
        {
        }

        public FastQueueClientOptions(FastQueueClientOptions options)
        {
            ServerUrl = options.ServerUrl;
        }
    }
}
