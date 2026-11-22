using System.Net;
using System.Net.Sockets;

namespace My_Redis;

public class Server
{
    private readonly TcpListener _listener;
    private readonly CommandProcessor _commandProcessor;
    public Server(RedisStore store,IPAddress address,int port)
    {
        _listener = new TcpListener(address,port);
        _commandProcessor = new CommandProcessor(store);
    }

    public async Task RunAsync()
    {
        _listener.Start();
        while (true)
        {
            var client = await _listener.AcceptSocketAsync();
            _= _commandProcessor.HandleClientAsync(client);
        }
    }
}
