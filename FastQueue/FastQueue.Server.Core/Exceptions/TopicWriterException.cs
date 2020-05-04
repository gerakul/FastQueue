using System;
using System.Runtime.Serialization;

namespace FastQueue.Server.Core.Exceptions
{
    public class TopicWriterException : Exception
    {
        public TopicWriterException()
        {
        }

        public TopicWriterException(string message) : base(message)
        {
        }

        public TopicWriterException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected TopicWriterException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}
