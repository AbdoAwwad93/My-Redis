using System.Text;
using My_Redis;
using My_Redis.src;

namespace My_Redis.Commands;

public sealed class PingCommand : ICommandHandler
{
    public string Name => "PING";
    public Task ExecuteAsync(byte[][] arguments, CommandContext context) => context.WriteAsync("+PONG\r\n").AsTask();
}

public sealed class EchoCommand : ICommandHandler
{
    public string Name => "ECHO";

    public Task ExecuteAsync(byte[][] arguments, CommandContext context)
    {
        if (arguments.Length != 2)
            return context.WriteAsync(RespWriter.Error("wrong number of arguments for 'echo' command")).AsTask();

        var value = arguments[1];
        var response = RespWriter.Encode($"${value.Length}\r\n").Concat(value).Concat(RespWriter.Encode("\r\n")).ToArray();
        return context.Client.WriteAsync(response).AsTask();
    }
}

public sealed class SetCommand : ICommandHandler
{
    public string Name => "SET";

    public async Task ExecuteAsync(byte[][] arguments, CommandContext context)
    {
        if (arguments.Length is not (3 or 5))
        {
            await context.WriteAsync(RespWriter.Error("invalid command options"));
            return;
        }

        var key = Helper.Decode(arguments[1]);
        var value = Helper.Decode(arguments[2]);
        if (arguments.Length == 3)
        {
            context.Store.Keys[key] = value;
            await context.WriteAsync("+OK\r\n");
            return;
        }

        var option = Helper.Decode(arguments[3]);
        if (!option.Equal("PX") && !option.Equal("EX"))
        {
            await context.WriteAsync(RespWriter.Error("not supported option"));
            return;
        }
        if (!int.TryParse(Helper.Decode(arguments[4]), out var time) || time <= 0)
        {
            await context.WriteAsync(RespWriter.Error("invalid expire value"));
            return;
        }

        context.Store.Keys[key] = value;
        await context.WriteAsync("+OK\r\n");
        var delay = option.Equal("PX") ? time : time * 1000;
        _ = Task.Run(async () =>
        {
            await Task.Delay(delay);
            context.Store.Keys.Remove(key, out _);
        });
    }
}

public sealed class GetCommand : ICommandHandler
{
    public string Name => "GET";

    public async Task ExecuteAsync(byte[][] arguments, CommandContext context)
    {
        if (arguments.Length != 2)
        {
            await context.WriteAsync(RespWriter.Error("wrong number of arguments for 'get' command"));
            return;
        }

        var key = Helper.Decode(arguments[1]);
        if (!context.Store.Keys.TryGetValue(key, out var value))
        {
            await context.WriteAsync("$-1\r\n");
            return;
        }

        await context.WriteAsync($"${Encoding.UTF8.GetByteCount(value)}\r\n{value}\r\n");
    }
}

public sealed class TypeCommand : ICommandHandler
{
    public string Name => "TYPE";

    public async Task ExecuteAsync(byte[][] arguments, CommandContext context)
    {
        if (arguments.Length != 2)
        {
            await context.WriteAsync(RespWriter.Error("wrong number of arguments for 'type' command"));
            return;
        }

        var key = Helper.Decode(arguments[1]);
        var type = context.Store.Keys.ContainsKey(key) ? "string"
            : context.Store.Streams.ContainsKey(key) ? "stream"
            : "none";
        await context.WriteAsync($"+{type}\r\n");
    }
}
