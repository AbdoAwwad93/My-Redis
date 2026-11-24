using System.Net.Sockets;
using My_Redis.Commands;
using My_Redis.RESP;
using My_Redis.src.Commands;
using My_Redis.src.Helpers;

namespace My_Redis;

public sealed class CommandProcessor
{
    private readonly IReadOnlyDictionary<string, ICommandHandler> _handlers;
    private readonly Store _store;

    public CommandProcessor(Store store)
    {
        _store = store;
        _handlers = new ICommandHandler[]
        {
            new PingCommand(),
            new EchoCommand(),
            new SetCommand(),
            new GetCommand(),
            new PushCommand("RPUSH", prepend: false),
            new PushCommand("LPUSH", prepend: true),
            new LRangeCommand(),
            new LLenCommand(),
            new LPopCommand(),
            new BLPopCommand(),
            new TypeCommand(),
            new XAddCommand(),
            new XRangeCommand(),
            new XReadCommand()
        }.ToDictionary(handler => handler.Name, StringComparer.OrdinalIgnoreCase);
    }

    public async Task HandleClientAsync(Socket client)
    {
        using var stream = new NetworkStream(client);
        var parser = new RespParser(stream);
        var context = new CommandContext(_store, stream);

        while (true)
        {
            var arguments = await parser.ReadCommandAsync();
            if (arguments is null)
                break;

            if (arguments.Length == 0)
            {
                await context.WriteAsync(RespWriter.Error("empty command"));
                continue;
            }

            var commandName = Helper.Decode(arguments[0]);
            if (!_handlers.TryGetValue(commandName, out var handler))
            {
                await context.WriteAsync(RespWriter.Error($"unknown command '{commandName}'"));
                continue;
            }

            await handler.ExecuteAsync(arguments, context);
        }
    }
}
