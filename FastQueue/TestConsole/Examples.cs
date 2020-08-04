using FastQueue.Client;
using FastQueue.Client.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TestConsole
{
    public class Examples
    {
        private MessageGenerator messageGenerator = new MessageGenerator(1);

        public static async Task RunAllTogether()
        {
            var topicName = "topic1";
            var subscriptionName1 = "subscription1";
            var subscriptionName2 = "subscription2";

            var examples = new Examples();

            using var client = examples.CreateClient();

            // uncomment if topic already exists
            //await client.DeleteTopic(topicName, true, default);

            await examples.CreateTopic(client, topicName);
            await examples.CreateSubscription(client, topicName, subscriptionName1);
            await examples.CreateSubscription(client, topicName, subscriptionName2);

            var subscriber1 = await examples.CreateSubscriber(client, topicName, subscriptionName1);
            var subscriber2 = await examples.CreateSubscriber(client, topicName, subscriptionName2);

            await examples.SimplePublish(client, topicName);
            await examples.PublishWithAsyncAckHandler(client, topicName);
            await examples.PublishBatches(client, topicName);
            await examples.PublishBatchesWithAsyncAckHandler(client, topicName);
            await examples.PublishAndWaitUntilAllMessagesAreAcknowledged(client, topicName);

            await subscriber1.DisposeAsync();
            await subscriber2.DisposeAsync();

            await examples.DeleteSubscription(client, topicName, subscriptionName1);
            await examples.DeleteSubscription(client, topicName, subscriptionName2);
            await examples.DeleteTopic(client, topicName);
        }

        public IFastQueueClient CreateClient()
        {
            var fastQueueClientOptions = new FastQueueClientOptions
            {
                ServerUrl = @"https://localhost:5001"
            };

            return new FastQueueClient(fastQueueClientOptions);
        }

        public Task CreateTopic(IFastQueueClient client, string topicName)
        {
            return client.CreateTopic(topicName, CancellationToken.None);
        }

        public Task DeleteTopic(IFastQueueClient client, string topicName)
        {
            return client.DeleteTopic(topicName, CancellationToken.None);
        }

        public Task CreateSubscription(IFastQueueClient client, string topicName, string subscriptionName)
        {
            return client.CreateSubscription(topicName, subscriptionName, CancellationToken.None);
        }

        public Task DeleteSubscription(IFastQueueClient client, string topicName, string subscriptionName)
        {
            return client.DeleteSubscription(topicName, subscriptionName, CancellationToken.None);
        }

        public async Task SimplePublish(IFastQueueClient client, string topicName)
        {
            await using var publisher = await client.CreatePublisher(topicName, ack =>
            {
                Console.WriteLine($"(simple) Last persisted SequenceNumber: {ack}");
            });

            for (int i = 0; i < 1000; i++)
            {
                var message = messageGenerator.CreateMessage().Serialize();
                var sequenceNumber = await publisher.Publish(message);
            }
        }

        public async Task PublishWithAsyncAckHandler(IFastQueueClient client, string topicName)
        {
            await using var publisher = await client.CreatePublisher(topicName, async ack =>
            {
                Console.WriteLine($"(async ack) Last persisted SequenceNumber: {ack}");
                await Task.CompletedTask;
            });

            for (int i = 0; i < 1000; i++)
            {
                var message = messageGenerator.CreateMessage().Serialize();
                var sequenceNumber = await publisher.Publish(message);
            }
        }

        public async Task PublishBatches(IFastQueueClient client, string topicName)
        {
            await using var publisher = await client.CreatePublisherMany(topicName, ack =>
            {
                Console.WriteLine($"(batches) Last persisted SequenceNumber: {ack}");
            });

            for (int i = 0; i < 1000; i++)
            {
                var messages = new ReadOnlyMemory<byte>[10];
                for (int j = 0; j < 10; j++)
                {
                    messages[j] = messageGenerator.CreateMessage().Serialize();
                }

                var sequenceNumber = await publisher.Publish(messages);
            }
        }

        public async Task PublishBatchesWithAsyncAckHandler(IFastQueueClient client, string topicName)
        {
            await using var publisher = await client.CreatePublisherMany(topicName, async ack =>
            {
                Console.WriteLine($"(batches async ack) Last persisted SequenceNumber: {ack}");
                await Task.CompletedTask;
            });

            for (int i = 0; i < 1000; i++)
            {
                var messages = new ReadOnlyMemory<byte>[10];
                for (int j = 0; j < 10; j++)
                {
                    messages[j] = messageGenerator.CreateMessage().Serialize();
                }

                var sequenceNumber = await publisher.Publish(messages);
            }
        }

        public async Task PublishAndWaitUntilAllMessagesAreAcknowledged(IFastQueueClient client, string topicName)
        {
            // in the future there will be an extensions for Publisher to do this in a simpler way
            bool waitingForAck = false;
            long lastPublishedSequenceNumber = 0;
            long lastAcknowledgedSequenceNumber = 0;
            TaskCompletionSource<int> tcs = null;
            await using var publisher = await client.CreatePublisher(topicName, ack =>
            {
                Console.WriteLine($"(waiting example) Last persisted SequenceNumber: {ack}");

                lastAcknowledgedSequenceNumber = ack;
                if (waitingForAck)
                {
                    if (lastPublishedSequenceNumber == ack)
                    {
                        tcs.TrySetResult(0);
                    }
                }
            });

            for (int i = 0; i < 1000; i++)
            {
                var message = messageGenerator.CreateMessage().Serialize();
                lastPublishedSequenceNumber = await publisher.Publish(message);
            }

            tcs = new TaskCompletionSource<int>();
            waitingForAck = true;
            if (lastAcknowledgedSequenceNumber != lastPublishedSequenceNumber)
            {
                await tcs.Task;
            }

            Console.WriteLine($"(waiting example) All messages acknowledged");
        }

        public Task<ISubscriber> CreateSubscriber(IFastQueueClient client, string topicName, string subscriptionName)
        {
            return client.CreateSubscriber(topicName, subscriptionName, async (sub, ms) =>
            {
                var arr = ms.ToArray();
                Console.WriteLine($"({subscriptionName}) Received: Count {arr.Length}, Range {arr[0].ID} - {arr[^1].ID}, Deserialized IDs {TestMessage.Deserialize(arr[0].Body.Span).ID} - {TestMessage.Deserialize(arr[^1].Body.Span).ID}");
                await sub.Complete(arr[^1].ID);
            });
        }
    }
}
