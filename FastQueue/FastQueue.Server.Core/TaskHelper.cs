using System;
using System.Threading.Tasks;

namespace FastQueue.Server.Core
{
    public static class TaskHelper
    {
        public static void FireAndForget(Func<Task> func)
        {
            Task.Run(func).ConfigureAwait(false);
        }
    }
}
