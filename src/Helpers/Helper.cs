using System;
using System.Text;

namespace My_Redis.src.Helpers;

public class Helper
{
    public static string Decode(byte[] bytes) => Encoding.UTF8.GetString(bytes); 
    public static int CompareId(long leftMilli, long leftSequence, long rightMilli, long rightSequence)
    {
        var byMilliseconds = leftMilli.CompareTo(rightMilli);
        return byMilliseconds != 0 ? byMilliseconds : leftSequence.CompareTo(rightSequence);
    }
    public static bool TryParseRangeBound(string bound, bool isStart, out long milliseconds, out long sequence)
    {
        if (bound == "-")
        {
            milliseconds = 0;
            sequence = 0;
            return true;
        }
        if (bound == "+")
        {
            milliseconds = long.MaxValue;
            sequence = long.MaxValue;
            return true;
        }

        var parts = bound.Split('-');
        if (parts.Length == 1 && long.TryParse(parts[0], out milliseconds) && milliseconds >= 0)
        {
            sequence = isStart ? 0 : long.MaxValue;
            return true;
        }
        if (parts.Length == 2 && long.TryParse(parts[0], out milliseconds) && milliseconds >= 0 &&
            long.TryParse(parts[1], out sequence) && sequence >= 0)
        {
            return true;
        }

        milliseconds = 0;
        sequence = 0;
        return false;
    }
}