using System;
using System.Diagnostics;
using System.IO;
using Couchbase;
using Couchbase.Configuration.Client;
using Couchbase.Core;
using Newtonsoft.Json;

namespace CouchbasePerfTest
{
    class Program
    {
        private static readonly string testDocument = File.ReadAllText(@"test-document.json");

        private const int itemCount = 1000;

        static void Main(string[] args)
        {
            DoPerfTest(false, "withoutcompression");
            DoPerfTest(true, "withcompression");
        }

        private static void DoPerfTest(bool useCompression, string bucketName)
        {
            var cluster = useCompression ?
                CreateClusterWithCompressionSerializer() : CreateClusterWithDefaultSerializer();

            var bucket = cluster.OpenBucket(bucketName);

            var manager = bucket.CreateManager("Administrator", "password");
            manager.Flush();

            var testObject = JsonConvert.DeserializeObject<RootObject>(testDocument);

            var totalReadTime = TimeSpan.Zero;
            var totalWriteTime = TimeSpan.Zero;

            var swTotal = Stopwatch.StartNew();

            for (int i = 0; i < itemCount; i++)
            {
                var key = Guid.NewGuid().ToString();

                var swRead = Stopwatch.StartNew();
                bucket.Insert(new Document<RootObject>
                {
                    Content = testObject,
                    Id = key
                });
                swRead.Stop();
                totalReadTime += swRead.Elapsed;

                var swWrite = Stopwatch.StartNew();
                var result = bucket.Get<RootObject>(key);
                swWrite.Stop();
                totalWriteTime += swWrite.Elapsed;
            }

            swTotal.Stop();

            Console.WriteLine("Total time elapsed: {0}", swTotal.Elapsed);
            Console.WriteLine("Avg write time: {0}", TimeSpan.FromMilliseconds(totalWriteTime.TotalMilliseconds / itemCount));
            Console.WriteLine("Avg read time: {0}", TimeSpan.FromMilliseconds(totalReadTime.TotalMilliseconds / itemCount));
        }

        private static ICluster CreateClusterWithDefaultSerializer()
        {
            return new Cluster();
        }

        private static ICluster CreateClusterWithCompressionSerializer()
        {
            return new Cluster(new ClientConfiguration
            {
                Serializer = () => new JsonAndGZipSerializer()
            });
        }
    }
}