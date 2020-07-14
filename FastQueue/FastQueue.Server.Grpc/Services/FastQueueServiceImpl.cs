using FastQueue.Server.Core.Abstractions;
using FastQueueService;
using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace FastQueue.Server.Grpc.Services
{
    public class FastQueueServiceImpl : FastQueueService.FastQueueService.FastQueueServiceBase
    {
        private readonly IServer server;

        public FastQueueServiceImpl(IServer server)
        {
            this.server = server;
        }

        public override Task<CreateTopicReply> CreateTopic(CreateTopicRequest request, ServerCallContext context)
        {
            var topic = server.GetTopic(request.Name);
            return Task.FromResult(new CreateTopicReply { Name = topic.GetSubscriptions().FirstOrDefault() });
        }
    }
}
