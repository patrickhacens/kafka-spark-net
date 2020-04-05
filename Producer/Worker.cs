using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Producer
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IProducer<Null, string> producer;

        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;
            ProducerConfig config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
            };
            producer = new ProducerBuilder<Null, string>(config).Build();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {

                    var result = await producer.ProduceAsync("b7f45352-6abf-436b-9c4a-98141699728c", new Message<Null, string>
                    {
                        Value = "valuezinho",
                    }).ConfigureAwait(false);

                    _logger.LogInformation("Worker running at: {time} sent value '{from}' to '{to}'", DateTimeOffset.Now, result.Value, result.TopicPartitionOffset);

                    await Task.Delay(50, stoppingToken);
                }
                catch (ProduceException<Null, string> ex)
                {
                    _logger.LogInformation("Delivery failed: {reason}", ex.Error.Reason);
                }
            }
        }
    }
}
