using System.Net.Sockets;
using My_Redis;

namespace My_Redis.Commands;

public interface ICommandHandler
{
    string Name { get; }
    Task ExecuteAsync(byte[][] arguments, Store store, NetworkStream stream);
}

