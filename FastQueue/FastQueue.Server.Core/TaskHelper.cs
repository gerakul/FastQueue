using System;
using System.Threading.Tasks;

namespace FastQueue.Server.Core
{
    internal static class TaskHelper
    {
        public static void FireAndForget(Func<Task> func)
        {
            Task.Run(func).ConfigureAwait(false);
        }

        public static void FireAndForget(Action action)
        {
            Task.Run(action).ConfigureAwait(false);
        }
    }
}
