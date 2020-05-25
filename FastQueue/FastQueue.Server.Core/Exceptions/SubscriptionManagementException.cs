using System;
using System.Runtime.Serialization;

namespace FastQueue.Server.Core.Exceptions
{
    public class SubscriptionManagementException : Exception
    {
        public SubscriptionManagementException()
        {
        }

        public SubscriptionManagementException(string message) : base(message)
        {
        }

        public SubscriptionManagementException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected SubscriptionManagementException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}
