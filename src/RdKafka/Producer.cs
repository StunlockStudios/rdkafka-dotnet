using System;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using RdKafka.Internal;

namespace RdKafka
{
    /// <summary>
    /// High-level, asynchronous message producer.
    /// </summary>
    public class Producer : Handle
    {
        public Producer(string brokerList) : this(null, brokerList) { }

        public Producer(Config config, string brokerList = null)
        {
            config = config ?? new Config();

            IntPtr cfgPtr = config.handle.Dup();
            //LibRdKafka.conf_set_dr_msg_cb(cfgPtr, DeliveryReportDelegate);
            Init(RdKafkaType.Producer, cfgPtr, config.Logger);

            if (brokerList != null)
            {
                handle.AddBrokers(brokerList);
            }
        }

        public Topic Topic(string topic, TopicConfig config = null)
        {
            return new Topic(handle, this, topic, config);
        }

        // Explicitly keep reference to delegate so it stays alive
        static LibRdKafka.DeliveryReportCallback DeliveryReportDelegate = DeliveryReportCallback;

        public delegate void DeliveryCallbackDelegate(IntPtr payloadPointer, IntPtr keyPointer, IntPtr opaque, Exception exception);
        public static event DeliveryCallbackDelegate OnDeliveryReport;

        static void DeliveryReportCallback(IntPtr rk,
                ref rd_kafka_message rkmessage, IntPtr opaque)
        {
            Exception exc = null;
            if (rkmessage.err != 0)
            {
                exc = RdKafkaException.FromErr(
                           rkmessage.err,
                           Marshal.PtrToStringAnsi(rkmessage.payload));
            }
            if (OnDeliveryReport != null)
            {
                OnDeliveryReport(rkmessage.payload, rkmessage.key, rkmessage._private, exc);
                return;
            }
            // msg_opaque was set by Topic.Produce
            var gch = GCHandle.FromIntPtr(rkmessage._private);
            var deliveryCompletionSource = (TaskCompletionSource<DeliveryReport>)gch.Target;
            gch.Free();

            if (exc != null)
            {
                deliveryCompletionSource.SetException(exc);
            }

            deliveryCompletionSource.SetResult(new DeliveryReport()
            {
                Offset = rkmessage.offset,
                Partition = rkmessage.partition
            });
        }
    }
}
