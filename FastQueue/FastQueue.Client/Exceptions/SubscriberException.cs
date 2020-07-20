using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace FastQueue.Client.Exceptions
{
    public class SubscriberException : Exception
    {
        public SubscriberException()
        {
        }

        public SubscriberException(string message) : base(message)
        {
        }

        public SubscriberException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected SubscriberException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}
