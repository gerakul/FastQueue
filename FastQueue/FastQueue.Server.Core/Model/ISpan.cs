using System;
using System.Collections.Generic;
using System.Text;

namespace FastQueue.Server.Core.Model
{
    public interface ISpan
    {
        ReadOnlySpan<byte> Span { get; }
    }
}
