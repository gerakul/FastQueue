using System;
using System.Runtime.Serialization;

namespace FastQueue.Server.Core.Exceptions
{
    public class TopicManagementException : Exception
    {
        public TopicManagementException()
        {
        }

        public TopicManagementException(string message) : base(message)
        {
        }

        public TopicManagementException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected TopicManagementException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}
