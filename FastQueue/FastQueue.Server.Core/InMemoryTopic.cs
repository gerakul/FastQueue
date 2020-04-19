using FastQueue.Server.Core.Model;
using System;
using System.Collections.Generic;
using System.Text;

namespace FastQueue.Server.Core
{
    public class InMemoryTopic : TopicBase
    {
        internal InMemoryTopic(string name) : base(name)
        {
        }
    }
}
