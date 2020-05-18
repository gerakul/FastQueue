using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace FastQueue.Server.Core.Exceptions
{
    public class FatalException : Exception
    {
        public FatalException()
        {
        }

        public FatalException(string message) : base(message)
        {
        }

        public FatalException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected FatalException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}
