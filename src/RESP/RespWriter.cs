using System.Text;

namespace My_Redis;

public static class RespWriter
{
    public static byte[] Encode(string response) => Encoding.UTF8.GetBytes(response);
    public static void AppendBulkString(StringBuilder response, string value)
    {
        response.Append('$').Append(Encoding.UTF8.GetByteCount(value)).Append("\r\n")
            .Append(value).Append("\r\n");
    }
}
