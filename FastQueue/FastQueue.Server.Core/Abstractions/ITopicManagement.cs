using System;
using System.Collections.Generic;
using System.Text;

namespace FastQueue.Server.Core.Abstractions
{
    public interface ITopicManagement : ITopic
    {
        void Restore();
        void Start();
        void Stop();
    }
}
