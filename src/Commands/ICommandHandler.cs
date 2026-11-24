using My_Redis.src.Commands;

namespace My_Redis.Commands;

public interface ICommandHandler
{
    string Name { get; }
    Task ExecuteAsync(byte[][] arguments, CommandContext context);
}

