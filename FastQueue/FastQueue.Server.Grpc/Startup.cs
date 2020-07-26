using FastQueue.Server.Core;
using FastQueue.Server.Core.Abstractions;
using FastQueue.Server.Grpc.Services;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace FastQueue.Server.Grpc
{
    public class Startup
    {
        private readonly IConfiguration configuration;

        public Startup(IConfiguration configuration)
        {
            this.configuration = configuration;
        }

        public void ConfigureServices(IServiceCollection services)
        {
            services.AddLogging(logging =>
            {
                logging.AddFilter("Microsoft", LogLevel.None);
                logging.AddFilter("Microsoft.Hosting.Lifetime", LogLevel.Information);
            });

            var fastQueueConfiguration = FastQueueConfiguration.Read(configuration);

            var topicFactory = new FileTopicFactory(fastQueueConfiguration.TopicFactoryOptions);
            var topicsConfigStorage = new TopicsConfigurationFileStorage(fastQueueConfiguration.TopicsConfigurationFileStorageOptions);

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
