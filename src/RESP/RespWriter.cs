using System.Text;

namespace My_Redis;

public static class RespWriter
{
    public static byte[] Encode(string response) => Encoding.UTF8.GetBytes(response);
}
