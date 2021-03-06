using System;
using System.Runtime.InteropServices;

namespace RdKafka.Internal
{
    enum MsgFlags
    {
        MSG_F_FREE = 1,
        MSG_F_COPY = 2
    }
 
    internal sealed class SafeTopicHandle : SafeHandleZeroIsInvalid
    {
        const int RD_KAFKA_PARTITION_UA = -1;

        internal SafeKafkaHandle kafkaHandle;

        private SafeTopicHandle() { }

        protected override bool ReleaseHandle()
        {
            LibRdKafka.topic_destroy(handle);
            // See SafeKafkaHandle.Topic
            kafkaHandle.DangerousRelease();
            return true;
        }

        internal string GetName() => Marshal.PtrToStringAnsi(LibRdKafka.topic_name(handle));

        internal long Produce(byte[] payload, int payloadOffset, int payloadCount, byte[] key, int keyOffset, int keyCount, int partition, IntPtr opaque, bool copyBuffer)
        {
            return (long) LibRdKafka.produce(
                    handle,
                    partition,
                    (IntPtr) (copyBuffer ? MsgFlags.MSG_F_COPY : 0),
                    payload != null ? Marshal.UnsafeAddrOfPinnedArrayElement(payload, payloadOffset) : IntPtr.Zero, (UIntPtr) payloadCount,
                    key != null ? Marshal.UnsafeAddrOfPinnedArrayElement(key, keyOffset) : IntPtr.Zero, (UIntPtr) keyCount,
                    opaque);
        }

        internal bool PartitionAvailable(int partition)
        {
            return LibRdKafka.topic_partition_available(handle, partition);
        }
    }
}
