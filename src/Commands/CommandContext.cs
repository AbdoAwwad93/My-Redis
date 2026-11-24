using System;
using System.Net.Sockets;

namespace My_Redis.src.Commands;

public sealed class CommandContext
{
    public Store Store { get; }
    public NetworkStream Client { get; }

    public CommandContext(Store store, NetworkStream client)
    {
        Store = store;
        Client = client;
    }

    public ValueTask WriteAsync(string response) => Client.WriteAsync(RespWriter.Encode(response));
    public ValueTask WriteAsync(byte[] response) => Client.WriteAsync(response);
}
