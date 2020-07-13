using Grpc.Net.Client;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FastQueue.Client
{
    public class FastQueueClient : IDisposable
    {
        private readonly GrpcChannel channel;
        private readonly FastQueueService.FastQueueService.FastQueueServiceClient grpcClient;

        public FastQueueClient(FastQueueClientOptions options)
        {
            channel = GrpcChannel.ForAddress(options.ServerUrl);
            grpcClient = new FastQueueService.FastQueueService.FastQueueServiceClient(channel);
        }

        public async Task<string> CreateTopic(string name, CancellationToken cancellationToken)
        {
            var reply = await grpcClient.CreateTopicAsync(new FastQueueService.CreateTopicRequest { Name = name }, cancellationToken: cancellationToken);
            return reply.Name;
        }

        public void Dispose()
        {
            channel?.Dispose();
        }
    }
}
