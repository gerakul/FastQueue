using FastQueue.Server.Core;
using FastQueue.Server.Core.Abstractions;
using FastQueue.Server.Grpc.Services;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace FastQueue.Server.Grpc
{
    public class Startup
    {
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddLogging(logging =>
            {
                logging.AddFilter("Microsoft", LogLevel.None);
                logging.AddFilter("Microsoft.Hosting.Lifetime", LogLevel.Information);
            });

            // ::: get from config file
            var topicFactory = new FileTopicFactory(new TopicFactoryOptions
            {
                DirectoryPath = @"C:\temp\storage",
                PersistentStorageFileLengthThreshold = 100 * 1024 * 1024,
                SubscriptionPointersStorageFileLengthThreshold = 10 * 1024 * 1024,
                TopicOptions = new TopicOptions
                {
                    PersistenceIntervalMilliseconds = 100,
                    PersistenceMaxFails = 100,
                    CleanupMaxFails = 10000,
                    SubscriptionPointersFlushMaxFails = 500,
                    DataArrayOptions = new InfiniteArrayOptions
                    {
                        BlockLength = 100000,
                        DataListCapacity = 128,
                        MinimumFreeBlocks = 20
                    }
                }
            });

            var topicsConfigStorage = new TopicsConfigurationFileStorage(new TopicsConfigurationFileStorageOptions
            {
                ConfigurationFile = @"C:\temp\storage\Topics.json"
            });

            var server = new Core.Server(topicFactory, topicsConfigStorage);

            services.AddSingleton<IServer>(server);

            services.AddSingleton<FastQueueServiceImpl>();
            services.AddHostedService<FastQueueCoreServer>();

            services.AddGrpc();
        }

        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseRouting();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapGrpcService<FastQueueServiceImpl>();

                endpoints.MapGet("/", async context =>
                {
                    await context.Response.WriteAsync("Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
                });
            });
        }
    }
}
