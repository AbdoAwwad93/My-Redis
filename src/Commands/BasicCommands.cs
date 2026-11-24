using System.Text;
using System.Net.Sockets;
using My_Redis.src.Helper;
using My_Redis.RESP;

namespace My_Redis.Commands;

public sealed class PingCommand : ICommandHandler
{
    public string Name => "PING";
    public Task ExecuteAsync(byte[][] arguments, Store store, NetworkStream stream) => stream.WriteResponseAsync("+PONG\r\n").AsTask();
}

public sealed class EchoCommand : ICommandHandler
{
    public string Name => "ECHO";

    public Task ExecuteAsync(byte[][] arguments, Store store, NetworkStream stream)
    {
        if (arguments.Length != 2)
            return stream.WriteResponseAsync(RespWriter.Error("wrong number of arguments for 'echo' command")).AsTask();

        var value = Helper.Decode(arguments[1]);
        var response = RespWriter.Encode($"${value.Length}\r\n{value}\r\n");
        return stream.WriteAsync(response).AsTask();
    }
}

public sealed class SetCommand : ICommandHandler
{
    public string Name => "SET";

    public async Task ExecuteAsync(byte[][] arguments, Store store, NetworkStream stream)
    {
        if (arguments.Length is not (3 or 5))
        {
            await stream.WriteResponseAsync(RespWriter.Error("invalid command options"));
            return;
        }

        var key = Helper.Decode(arguments[1]);
        var value = Helper.Decode(arguments[2]);
        if (arguments.Length == 3)
        {
            store.Keys[key] = value;
            await stream.WriteResponseAsync("+OK\r\n");
            return;
        }

        var option = Helper.Decode(arguments[3]);
        if (!option.Equal("PX") && !option.Equal("EX"))
        {
            await stream.WriteResponseAsync(RespWriter.Error("not supported option"));
            return;
        }
        if (!int.TryParse(Helper.Decode(arguments[4]), out var time) || time <= 0)
        {
            await stream.WriteResponseAsync(RespWriter.Error("invalid expire value"));
            return;
        }

        store.Keys[key] = value;
        await stream.WriteResponseAsync("+OK\r\n");
        var delay = option.Equal("PX") ? time : time * 1000;
        _ = Task.Run(async () =>
        {
            await Task.Delay(delay);
            store.Keys.Remove(key, out _);
        });
    }
}

public sealed class GetCommand : ICommandHandler
{
    public string Name => "GET";

    public async Task ExecuteAsync(byte[][] arguments, Store store, NetworkStream stream)
    {
        if (arguments.Length != 2)
        {
            await stream.WriteResponseAsync(RespWriter.Error("wrong number of arguments for 'get' command"));
            return;
        }

        var key = Helper.Decode(arguments[1]);
        if (!store.Keys.TryGetValue(key, out var value))
        {
            await stream.WriteResponseAsync("$-1\r\n");
            return;
        }

        await stream.WriteResponseAsync($"${Encoding.UTF8.GetByteCount(value)}\r\n{value}\r\n");
    }
}

public sealed class TypeCommand : ICommandHandler
{
    public string Name => "TYPE";

    public async Task ExecuteAsync(byte[][] arguments, Store store, NetworkStream stream)
    {
        if (arguments.Length != 2)
        {
            await stream.WriteResponseAsync(RespWriter.Error("wrong number of arguments for 'type' command"));
            return;
        }

        var key = Helper.Decode(arguments[1]);
        var type = store.Keys.ContainsKey(key) ? "string"
            : store.Streams.ContainsKey(key) ? "stream"
            : "none";
        await stream.WriteResponseAsync($"+{type}\r\n");
    }
}
