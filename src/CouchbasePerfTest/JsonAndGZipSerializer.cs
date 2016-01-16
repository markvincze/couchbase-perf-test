using System;
using System.IO;
using System.IO.Compression;
using Couchbase.Core.Serialization;
using Newtonsoft.Json;

namespace CouchbasePerfTest
{
    /// <summary>
    /// A custom <see cref="ITypeSerializer"/> for the Couchbase .NET SDK that uses Json.NET and GZip compression for serialization.
    /// The implementation is based on the one found here: http://blog.couchbase.com/2015/june/using-jil-for-custom-json-serialization-in-the-couchbase-.net-sdk
    /// </summary>
    public class JsonAndGZipSerializer : ITypeSerializer
    {
        private readonly JsonSerializer jsonSerializer;

        public JsonAndGZipSerializer()
        {
            this.jsonSerializer = new JsonSerializer();
        }

        /// <summary>
        /// Deserializes the specified stream into the <see cref="T:System.Type" /> T specified as a generic parameter.
        /// </summary>
        /// <typeparam name="T">The <see cref="T:System.Type" /> specified as the type of the value.</typeparam>
        /// <param name="stream">The stream.</param>
        /// <returns>
        /// The <see cref="T:System.Type" /> instance representing the value of the key.
        /// </returns>
        public T Deserialize<T>(Stream stream)
        {
            using (var decompressedStream = Decompress(stream))
            {
                using (var sr = new StreamReader(decompressedStream))
                {
                    using (var jtr = new JsonTextReader(sr))
                    {
                        return jsonSerializer.Deserialize<T>(jtr);
                    }
                }
            }
        }

        /// <summary>
        /// Deserializes the specified buffer into the <see cref="T:System.Type" /> T specified as a generic parameter.
        /// </summary>
        /// <typeparam name="T">The <see cref="T:System.Type" /> specified as the type of the value.</typeparam>
        /// <param name="buffer">The buffer to deserialize from.</param>
        /// <param name="offset">The offset of the buffer to start reading from.</param>
        /// <param name="length">The length of the buffer to read from.</param>
        /// <returns>
        /// The <see cref="T:System.Type" /> instance representing the value of the key.
        /// </returns>
        public T Deserialize<T>(byte[] buffer, int offset, int length)
        {
            T value = default(T);
            if (length == 0)
            {
                return value;
            }

            using (var ms = new MemoryStream(buffer, offset, length))
            {
                using (var decompressedStream = Decompress(ms))
                {
                    using (var sr = new StreamReader(decompressedStream))
                    {
                        using (var jtr = new JsonTextReader(sr))
                        {
                            // Use the following code block only for value types.
                            // Strangely enough Nullable<T> itself is a value type so we need to filter it out.
                            if (typeof(T).IsValueType && (!typeof(T).IsGenericType
                                                           || typeof(T).GetGenericTypeDefinition() != typeof(Nullable<>)))
                            {
                                // We can't declare Nullable<T> because T is not restricted to struct in this method scope.
                                var type = typeof(Nullable<>).MakeGenericType(typeof(T));

                                object nullableVal = jsonSerializer.Deserialize(jtr, type);

                                // Either we have a null or an instance of Nullabte<T> that can be cast directly to T.
                                value = nullableVal == null ? default(T) : (T)nullableVal;
                            }
                            else
                            {
                                value = jsonSerializer.Deserialize<T>(jtr);
                            }
                        }
                    }
                }
            }

            return value;
        }

        /// <summary>
        /// Serializes the specified object into a buffer.
        /// </summary>
        /// <param name="obj">The object to serialize.</param>
        /// <returns>
        /// A <see cref="T:System.Byte" /> array that is the serialized value of the key.
        /// </returns>
        public byte[] Serialize(object obj)
        {
            using (var ms = new MemoryStream())
            {
                using (var sw = new StreamWriter(ms))
                {
                    using (var jtw = new JsonTextWriter(sw))
                    {
                        jsonSerializer.Serialize(jtw, obj);
                        sw.Flush();

                        ms.Position = 0;
                        return Compress(ms);
                    }
                }
            }
        }

        private byte[] Compress(Stream stream)
        {
            using (var mso = new MemoryStream())
            {
                using (var gs = new GZipStream(mso, CompressionMode.Compress))
                {
                    stream.CopyTo(gs);
                }

                return mso.ToArray();
            }
        }

        private Stream Decompress(Stream compressed)
        {
            var mso = new MemoryStream();

            using (var gs = new GZipStream(compressed, CompressionMode.Decompress))
            {
                gs.CopyTo(mso);
            }

            mso.Position = 0;

            return mso;
        }
    }
}