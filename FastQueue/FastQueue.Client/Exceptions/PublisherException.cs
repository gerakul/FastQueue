using System;
using System.Runtime.Serialization;

namespace FastQueue.Client.Exceptions
{
    public class PublisherException : Exception
    {
        public PublisherException()
        {
        }

        public PublisherException(string message) : base(message)
        {
        }

        public PublisherException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected PublisherException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}
