using System.Text;
using System.Net.Sockets;
using My_Redis.src.Helper;

namespace My_Redis.Commands;

public sealed class PushCommand : ICommandHandler
{
    private readonly bool _prepend;
    public string Name { get; }

    public PushCommand(string name, bool prepend)
    {
        Name = name;
        _prepend = prepend;
    }

    public async Task ExecuteAsync(byte[][] arguments, Store store, NetworkStream stream)
    {
        if (arguments.Length < 3)
        {
            await stream.WriteResponseAsync(RespWriter.Error("wrong number of arguments"));
            return;
        }

        var key = Helper.Decode(arguments[1]);
        var listLock = store.ListLocks.GetOrAdd(key, _ => new object());
        int resultLength;
        lock (listLock)
        {
            var list = store.Lists.GetOrAdd(key, _ => new List<string>());
            resultLength = list.Count;
            for (var i = 2; i < arguments.Length; i++)
            {
                var value = Helper.Decode(arguments[i]);
                resultLength++;
                var delivered = false;
                if (store.WaitingClients.TryGetValue(key, out var waiters))
                {
                    while (waiters.Count > 0)
                    {
                        var waiter = waiters.Dequeue();
                        if (waiter.TrySetResult(value))
                        {
                            delivered = true;
                            break;
                        }
                    }
                }

                if (!delivered)
                {
                    if (_prepend) list.Insert(0, value);
                    else list.Add(value);
                }
            }
        }

        await stream.WriteResponseAsync($":{resultLength}\r\n");
    }
}

public sealed class LRangeCommand : ICommandHandler
{
    public string Name => "LRANGE";

    public async Task ExecuteAsync(byte[][] arguments, Store store, NetworkStream stream)
    {
        if (arguments.Length != 4)
        {
            await stream.WriteResponseAsync(RespWriter.Error("wrong number of arguments for 'lrange' command"));
            return;
        }

        var key = Helper.Decode(arguments[1]);
        if (!int.TryParse(Helper.Decode(arguments[2]), out var start) ||
            !int.TryParse(Helper.Decode(arguments[3]), out var stop))
        {
            await stream.WriteResponseAsync(RespWriter.Error("invalid start or stop arguments"));
            return;
        }

        var listLock = store.ListLocks.GetOrAdd(key, _ => new object());
        string[] values;
        lock (listLock)
        {
            if (!store.Lists.TryGetValue(key, out var list) || list.Count == 0)
            {
                values = Array.Empty<string>();
            }
            else
            {
                var count = list.Count;
                if (start < 0) start = count + start;
                if (stop < 0) stop = count + stop;
                start = Math.Max(start, 0);
                stop = Math.Min(stop, count - 1);
                values = start > stop || start >= count
                    ? Array.Empty<string>()
                    : list.Skip(start).Take(stop - start + 1).ToArray();
            }
        }

        var response = new StringBuilder($"*{values.Length}\r\n");
        foreach (var value in values)
            RespWriter.AppendBulkString(response, value);
        await stream.WriteResponseAsync(response.ToString());
    }
}

public sealed class LLenCommand : ICommandHandler
{
    public string Name => "LLEN";

    public async Task ExecuteAsync(byte[][] arguments, Store store, NetworkStream stream)
    {
        if (arguments.Length != 2)
        {
            await stream.WriteResponseAsync(RespWriter.Error("wrong number of arguments for 'llen' command"));
            return;
        }

        var key = Helper.Decode(arguments[1]);
        var listLock = store.ListLocks.GetOrAdd(key, _ => new object());
        int length;
        lock (listLock)
            length = store.Lists.TryGetValue(key, out var list) ? list.Count : 0;
        await stream.WriteResponseAsync($":{length}\r\n");
    }
}

public sealed class LPopCommand : ICommandHandler
{
    public string Name => "LPOP";

    public async Task ExecuteAsync(byte[][] arguments, Store store, NetworkStream stream)
    {
        if (arguments.Length is < 2 or > 3)
        {
            await stream.WriteResponseAsync(RespWriter.Error("wrong number of arguments for 'lpop' command"));
            return;
        }

        var countProvided = arguments.Length == 3;
        var count = 1;
        if (countProvided && (!int.TryParse(Helper.Decode(arguments[2]), out count) || count < 0))
        {
            await stream.WriteResponseAsync(RespWriter.Error("invalid count"));
            return;
        }

        var key = Helper.Decode(arguments[1]);
        var listLock = store.ListLocks.GetOrAdd(key, _ => new object());
        string[] popped;
        lock (listLock)
        {
            if (!store.Lists.TryGetValue(key, out var list) || list.Count == 0 || count == 0)
            {
                popped = Array.Empty<string>();
            }
            else
            {
                var amount = Math.Min(count, list.Count);
                popped = list.Take(amount).ToArray();
                list.RemoveRange(0, amount);
            }
        }

        if (!countProvided)
        {
            if (popped.Length == 0)
                await stream.WriteResponseAsync("$-1\r\n");
            else
            {
                var response = new StringBuilder();
                RespWriter.AppendBulkString(response, popped[0]);
                await stream.WriteResponseAsync(response.ToString());
            }
            return;
        }

        var arrayResponse = new StringBuilder($"*{popped.Length}\r\n");
        foreach (var value in popped)
            RespWriter.AppendBulkString(arrayResponse, value);
        await stream.WriteResponseAsync(arrayResponse.ToString());
    }
}

public sealed class BLPopCommand : ICommandHandler
{
    public string Name => "BLPOP";

    public async Task ExecuteAsync(byte[][] arguments, Store store, NetworkStream stream)
    {
        if (arguments.Length != 3 || !double.TryParse(Helper.Decode(arguments[2]), out var timeout) || timeout < 0)
        {
            await stream.WriteResponseAsync(RespWriter.Error("invalid BLPOP arguments"));
            return;
        }

        var key = Helper.Decode(arguments[1]);
        var listLock = store.ListLocks.GetOrAdd(key, _ => new object());
        var waiter = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        string? immediateValue = null;
        lock (listLock)
        {
            if (store.Lists.TryGetValue(key, out var list) && list.Count > 0)
            {
                immediateValue = list[0];
                list.RemoveAt(0);
            }
            else
            {
                var waiters = store.WaitingClients.GetOrAdd(key, _ => new Queue<TaskCompletionSource<string>>());
                waiters.Enqueue(waiter);
            }
        }

        if (immediateValue is not null)
        {
            await WritePairAsync(stream, key, immediateValue);
            return;
        }

        try
        {
            var value = timeout == 0
                ? await waiter.Task
                : await waiter.Task.WaitAsync(TimeSpan.FromSeconds(timeout));
            await WritePairAsync(stream, key, value);
        }
        catch (TimeoutException)
        {
            string? deliveredValue = null;
            lock (listLock)
            {
                if (waiter.Task.IsCompletedSuccessfully)
                    deliveredValue = waiter.Task.Result;
                else if (store.WaitingClients.TryGetValue(key, out var queued))
                {
                    var remaining = queued.Where(item => !ReferenceEquals(item, waiter)).ToList();
                    queued.Clear();
                    foreach (var item in remaining) queued.Enqueue(item);
                }
            }

            if (deliveredValue is null)
                await stream.WriteResponseAsync("*-1\r\n");
            else
                await WritePairAsync(stream, key, deliveredValue);
        }
    }

    private static Task WritePairAsync(NetworkStream stream, string key, string value)
    {
        var response = new StringBuilder("*2\r\n");
        RespWriter.AppendBulkString(response, key);
        RespWriter.AppendBulkString(response, value);
        return stream.WriteResponseAsync(response.ToString()).AsTask();
    }
}
