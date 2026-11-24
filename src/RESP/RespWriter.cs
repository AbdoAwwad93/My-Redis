using System.Text;

namespace My_Redis.RESP;

public static class RespWriter
{
    public static byte[] Encode(string response) => Encoding.UTF8.GetBytes(response);
     public static void AppendBulkString(StringBuilder response, string value)
    {
        response.Append('$').Append(Encoding.UTF8.GetByteCount(value)).Append("\r\n")
            .Append(value).Append("\r\n");
    }
    public static byte[] Error(string text)=> Encoding.UTF8.GetBytes($"-ERR {text}\r\n");
    public static byte[] EmptyArray()=> Encoding.UTF8.GetBytes("*0\r\n");
}
