using Google.Protobuf.Reflection;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

namespace TestConsole
{
    public class MessageGenerator
    {
        private const int BodiesNumber = 10000;

        private long seed;
        private Random rnd;
        private byte[][] bodies;

        public MessageGenerator(long seed)
        {
            this.seed = seed;

            rnd = new Random((int)DateTime.UtcNow.Ticks);
            bodies = new byte[BodiesNumber][];
            for (int i = 0; i < bodies.Length; i++)
            {
                bodies[i] = new byte[rnd.Next(50, 150)];
                rnd.NextBytes(bodies[i]);
            }
        }

        public TestMessage CreateMessage()
        {
            return new TestMessage(seed++, DateTime.UtcNow, bodies[rnd.Next(0, BodiesNumber - 1)]);
        }
    }

    public readonly struct TestMessage
    {
        public readonly long ID;
        public readonly DateTime Timestamp;
        public readonly byte[] Body;

        public TestMessage(long id, DateTime timestamp, byte[] body)
        {
            ID = id;
            Timestamp = timestamp;
            Body = body;
        }

        public byte[] Serialize()
        {
            var bytes = new byte[Body.Length + 16];
            LongToBytes(ID, bytes.AsSpan(0));
            LongToBytes(Timestamp.Ticks, bytes.AsSpan(8));
            Body.AsSpan().CopyTo(bytes.AsSpan(16));
            return bytes;
        }

        public static TestMessage Deserialize(ReadOnlySpan<byte> span)
        {
            var id = BytesToLong(span);
            var time = new DateTime(BytesToLong(span.Slice(8)), DateTimeKind.Utc);
            var body = new byte[span.Length - 16];
            span.Slice(16).CopyTo(body);
            return new TestMessage(id, time, body);
        }

        private static void LongToBytes(long x, Span<byte> span)
        {
            span[0] = (byte)x;
            span[1] = (byte)(x >> 8);
            span[2] = (byte)(x >> 16);
            span[3] = (byte)(x >> 24);
            span[4] = (byte)(x >> 32);
            span[5] = (byte)(x >> 40);
            span[6] = (byte)(x >> 48);
            span[7] = (byte)(x >> 56);
        }

        private static long BytesToLong(ReadOnlySpan<byte> span)
        {
            return (long)span[0]
                | ((long)span[1] << 8)
                | ((long)span[2] << 16)
                | ((long)span[3] << 24)
                | ((long)span[4] << 32)
                | ((long)span[5] << 40)
                | ((long)span[6] << 48)
                | ((long)span[7] << 56);
        }
    }
}
