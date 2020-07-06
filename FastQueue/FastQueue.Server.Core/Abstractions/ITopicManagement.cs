using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace FastQueue.Server.Core.Abstractions
{
    public interface ITopicManagement : ITopic
    {
        string Name { get; }
        void Restore();
        void Start();
        Task Stop(bool checkForSubscriptions);
    }
}
