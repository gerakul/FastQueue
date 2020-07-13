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
        public FastQueueServiceImpl()
        {
        }

        public override Task<CreateTopicReply> CreateTopic(CreateTopicRequest request, ServerCallContext context)
        {
            return Task.FromResult(new CreateTopicReply { Name = request.Name });
        }
    }
}
