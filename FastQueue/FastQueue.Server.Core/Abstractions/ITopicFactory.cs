using System;
using System.Collections.Generic;
using System.Text;

namespace FastQueue.Server.Core.Abstractions
{
    public interface ITopicFactory
    {
        ITopicManagement CreateTopic(string name);
        void DeleteTopic(ITopicManagement topic);
    }
}
