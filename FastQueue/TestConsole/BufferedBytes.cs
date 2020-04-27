using FastQueue.Server.Core.Model;
using System;
using System.Collections.Generic;
using System.Text;

namespace TestConsole
{
    public class BufferedBytes : ISpan
    {
        private byte[] buffer;
        private int start;
        private int length;

        public BufferedBytes(byte[] buffer, int start, int length)
        {
            this.buffer = buffer;
            this.start = start;
            this.length = length;
        }

        public ReadOnlySpan<byte> Span => buffer.AsSpan(start, length);
    }
}
