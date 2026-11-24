using System.Net.Sockets;
using My_Redis;
using My_Redis.RESP;

namespace My_Redis.src.Helper;

public static class CommandResponseExtensions
{
    public static ValueTask WriteResponseAsync(this NetworkStream stream, string response) =>
        stream.WriteAsync(RespWriter.Encode(response));

    public static ValueTask WriteResponseAsync(this NetworkStream stream, byte[] response) =>
        stream.WriteAsync(response);
}
