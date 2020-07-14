using FastQueue.Server.Core;
using FastQueue.Server.Core.Abstractions;
using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace FastQueue.Server.Grpc
{
    public class FastQueueCoreServer : IHostedService
    {
        private IServer server;

        public FastQueueCoreServer(IServer server)
        {
            this.server = server;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            server.Restore();
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return server.Stop();
        }
    }
}
