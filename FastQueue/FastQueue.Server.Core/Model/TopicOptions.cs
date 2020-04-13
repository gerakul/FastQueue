using System;
using System.Collections.Generic;
using System.Text;

namespace FastQueue.Server.Core.Model
{
    public class TopicOptions
    {
        public bool Persistent { get; }

        public TopicOptions(bool persistent)
        {
            Persistent = persistent;
        }

        public static readonly TopicOptions Default = new TopicOptions(false);
    }
}
