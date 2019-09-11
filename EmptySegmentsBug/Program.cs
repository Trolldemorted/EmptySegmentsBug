using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.IO.Pipelines;
using System.Threading.Tasks;

namespace EmptySegmentsBug
{
    class Program
    {
        static readonly FileStream fs = File.Open(@"../../../test.blob", FileMode.Open, FileAccess.Read, FileShare.Read);

        static void Main(string[] args)
        {
            Test().Wait();
        }

        async static Task Test()
        {
            var pipe = new Pipe();
            var consumer = Task.Run(() => Consume(pipe.Reader));
            var inflateInputStream = new MemoryStream();
            var inflateOutputStream = new DeflateStream(inflateInputStream, CompressionMode.Decompress);
            try
            {
                while (true)
                {
                    var lenBytes = ForceRead(4);
                    if (lenBytes == null)
                        break;
                    var len = BitConverter.ToInt32(lenBytes);
                    var buf = ForceRead(len);
                    inflateInputStream.Position = 0;
                    inflateInputStream.Write(buf);
                    inflateInputStream.Position = 0;
                    inflateInputStream.SetLength(buf.Length);

                    int read;
                    do
                    {
                        Memory<byte> memory = pipe.Writer.GetMemory(buf.Length * 2);
                        read = inflateOutputStream.Read(memory.Span);
                        pipe.Writer.Advance(read);
                    }
                    while (read != 0);
                    await pipe.Writer.FlushAsync();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"Filling pipe failed: {e}\n{e.Message}");
            }
            await pipe.Writer.CompleteAsync();
            Console.WriteLine("Writer finished");
            await consumer;
        }

        static async Task Consume(PipeReader reader)
        {
            try
            {
                while (true)
                {
                    var result = await reader.ReadAsync();
                    if (result.Buffer.Length == 0 && result.IsCanceled)
                        break;
                    if (result.Buffer.First.Length == 0)
                        throw new InvalidDataException("first segment empty!");

                    foreach (var segment in result.Buffer)
                    {
                        if (segment.Length == 0)
                            throw new InvalidDataException("segment empty!");
                    }
                    reader.AdvanceTo(result.Buffer.End);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"Consuming pipe failed: {e}\n{e.Message}");
            }
        }

        static byte[] ForceRead(int length)
        {
            byte[] buf = new byte[length];
            int read = 0;
            do
            {
                read = fs.Read(buf, read, length - read);
                if (read == 0)
                    return null;
            }
            while (read < length);
            
            return buf;
        }
    }
}
