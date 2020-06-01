using FastQueue.Server.Core.Model;
using System;
using System.Collections.Generic;
using System.Text;

namespace FastQueue.Server.Core
{
    internal class DataSnapshot
    {
        public long StartMessageId;
        public ReadOnlyMemory<Message>[] Data;
    }
}
