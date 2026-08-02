using System.Text;

namespace KubeJob.Core.Linq;

/// <summary>
/// Computes CRC16-CCITT for consistent hashing across platform boundaries.
/// CRC16 is deliberately chosen (over FNV-1a) because its output is a flat
/// 16-bit unsigned value; the modulo into 16384 virtual slots produces
/// reproducible outcome identical to Redis Cluster's HASH_SLOT distribution.
/// </summary>
internal static class Crc16
{
    private const ushort Polynomial = 0x1021;
    private const ushort InitialValue = 0xFFFF;

    public static ushort Compute(string value)
    {
        return Compute(Encoding.UTF8.GetBytes(value));
    }

    public static ushort Compute(ReadOnlySpan<byte> bytes)
    {
        ushort crc = InitialValue;
        // ReSharper disable once ForCanBeConvertedToForeach — span-based loop
        for (var i = 0; i < bytes.Length; i++)
        {
            crc ^= (ushort)(bytes[i] << 8);
            for (var bit = 0; bit < 8; bit++)
            {
                if ((crc & 0x8000) != 0)
                {
                    crc = (ushort)((crc << 1) ^ Polynomial);
                }
                else
                {
                    crc <<= 1;
                }
            }
        }

        return crc;
    }
}
