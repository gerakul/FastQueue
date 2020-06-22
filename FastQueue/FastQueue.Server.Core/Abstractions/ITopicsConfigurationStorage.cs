using System;
using System.Collections.Generic;
using System.Text;

namespace FastQueue.Server.Core.Abstractions
{
    public interface ITopicsConfigurationStorage
    {
        void Update(TopicsConfiguration topicsConfiguration);
        TopicsConfiguration Read();
    }
}
