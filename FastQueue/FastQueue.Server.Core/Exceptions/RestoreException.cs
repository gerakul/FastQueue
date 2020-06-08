using System;
using System.Runtime.Serialization;

namespace FastQueue.Server.Core.Exceptions
{
    public class RestoreException : Exception
    {
        public RestoreException()
        {
        }

        public RestoreException(string message) : base(message)
        {
        }

        public RestoreException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected RestoreException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}
