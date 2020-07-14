using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace FastQueue.Server.Core.Abstractions
{
    public interface IServer
    {
        void Restore();
        Task Stop();
        void CreateNewTopic(string name);
        Task DeleteTopic(string name, bool deleteSubscriptions = false);
        ITopic GetTopic(string name);
    }
}
