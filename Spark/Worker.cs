using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Streaming;

namespace Spark
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly SparkSession spark;
        private readonly DataFrame input;

        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;
            spark = SparkSession.Builder()
                    .AppName("meuovo")
                    .GetOrCreate();
            input = spark.ReadStream()
                    .Format("kafka")
                    .Option("kafka.bootstrap.servers", "localhost:9092")
                    .Option("subscribe", "b7f45352-6abf-436b-9c4a-98141699728c")
                    .Load()
                    .SelectExpr("CAST(value AS STRING)");
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                StreamingQuery query = input.WriteStream()
                    .OutputMode(Microsoft.Spark.Sql.Streaming.OutputMode.Append)
                    .Format("console")
                    .Start();

                query.AwaitTermination();
            }
            return Task.CompletedTask;
        }
    }
}
