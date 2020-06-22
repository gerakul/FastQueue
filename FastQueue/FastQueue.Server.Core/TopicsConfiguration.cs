using System;
using System.Collections.Generic;
using System.Text;

namespace FastQueue.Server.Core
{
    public class TopicsConfiguration
    {
        public List<TopicConfiguration> Topics { get; set; }
    }

    public class TopicConfiguration
    {
        public string Name { get; set; }
    }
}
