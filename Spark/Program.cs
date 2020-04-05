using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Streaming;

namespace Spark
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var spark = SparkSession.Builder()
                    .AppName("meuovo")
                    .GetOrCreate();
            var input = spark.ReadStream()
                    .Format("kafka")
                    .Option("kafka.bootstrap.servers", "localhost:9092")
                    .Option("subscribe", "b7f45352-6abf-436b-9c4a-98141699728c")
                    .Load()
                    .SelectExpr("CAST(value AS STRING)");

            StreamingQuery query = input.WriteStream()
                    .OutputMode(OutputMode.Append)
                    .Format("console")
                    .Start();

            query.AwaitTermination();
        }

        
    }
}
