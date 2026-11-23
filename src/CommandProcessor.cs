using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Text;

namespace My_Redis;

public class CommandProcessor
{
    private readonly Store _store;
    public CommandProcessor(Store store) => _store = store;
    public string Decode(byte[] bytes) => Encoding.UTF8.GetString(bytes);
    public async Task HandleClientAsync(Socket client)
    {
        using var stream = new NetworkStream(client);
        var parser = new RespParser(stream);
        while (true)
        {
            var arguments = await parser.ReadCommandAsync();
            if (arguments == null) break;
            var command = Decode(arguments[0]);
            if (command.Equal("PING"))
            {
                byte[] response = RespWriter.Encode("+PONG\r\n");
                await stream.WriteAsync(response);
            }
            else if (command.Equal("ECHO"))
            {
                var arg = arguments[1];
                var header = RespWriter.Encode($"${arg.Length}\r\n");
                var footer = RespWriter.Encode("\r\n");
                await stream.WriteAsync(header);
                await stream.WriteAsync(arg);
                await stream.WriteAsync(footer);
            }
            else if (command.Equal("SET"))
            {
                var key = Decode(arguments[1]);
                var value = Decode(arguments[2]);
                if (arguments.Length == 3)
                {
                    _store.Keys[key] = value;
                    var response = RespWriter.Encode("+OK\r\n");
                    await stream.WriteAsync(response);
                }
                else if (arguments.Length == 5)
                {
                    var option = Decode(arguments[3]);
                    if (!option.Equal("PX") && !option.Equal("EX"))
                    {
                        await stream.WriteAsync(RespWriter.Encode("-ERR not supported option\r\n"));
                        continue;
                    }
                    if (int.TryParse(Decode(arguments[4]), out var time) && time > 0)
                    {
                        _store.Keys[key] = value;
                        var response = RespWriter.Encode("+OK\r\n");
                        await stream.WriteAsync(response);
                        if (option.Equal("PX"))
                        {
                            _ = Task.Run(async () =>
                            {
                                await Task.Delay(time);
                                _store.Keys.Remove(key, out var val);
                            });
                        }
                        else if (option.Equal("EX"))
                        {
                            _ = Task.Run(async () =>
                            {
                                await Task.Delay(time * 1000);
                                _store.Keys.Remove(key, out var val);
                            });
                        }

                    }
                    else
                    {
                        await stream.WriteAsync(RespWriter.Encode("-ERR invalid expire value\r\n"));
                    }
                }
                else
                {
                    await stream.WriteAsync(RespWriter.Encode("-ERR invalid command options\r\n"));
                }

            }
            else if (command.Equal("GET"))
            {

                var key = Decode(arguments[1]);
                if (_store.Keys.TryGetValue(key, out var value))
                {
                    var len = Encoding.UTF8.GetByteCount(value);
                    var response = RespWriter.Encode($"${len}\r\n{value}\r\n");
                    await stream.WriteAsync(response);
                }
                else
                {
                    var response = RespWriter.Encode($"$-1\r\n");
                    await stream.WriteAsync(response);
                }
            }
            else if (command.Equal("RPUSH"))
            {
                if (arguments.Length < 3)
                {
                    await stream.WriteAsync(RespWriter.Encode("-ERR invalid command argumentss\r\n"));
                    continue;
                }
                var listName = Decode(arguments[1]);
                var listLock = _store.ListLocks.GetOrAdd(listName, _ => new object());
                int resultLength;
                lock (listLock)
                {
                    var list = _store.Lists.GetOrAdd(listName, _ => new List<string>());
                    resultLength = list.Count;
                    for (int i = 2; i < arguments.Length; i++)
                    {
                        var value = Decode(arguments[i]);
                        resultLength++;
                        var delivered = false;
                        if (_store.WaitingClients.TryGetValue(listName, out var waiters))
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
                        if (!delivered) list.Add(value);
                    }
                }
                await stream.WriteAsync(RespWriter.Encode($":{resultLength}\r\n"));

            }
            else if (command.Equal("LRANGE"))
            {
                if (arguments.Length < 4)
                {
                    await stream.WriteAsync(RespWriter.Encode("-ERR invalid command argumentss\r\n"));
                }
                else
                {
                    var listName = Decode(arguments[1]);
                    var start = Decode(arguments[2]);
                    var stop = Decode(arguments[3]);
                    if (!int.TryParse(start, out var startval) || !int.TryParse(stop, out var stopval))
                    {
                        await stream.WriteAsync(RespWriter.Encode("-ERR invalid start or stop arguments\r\n"));
                        continue;
                    }
                    if (startval < 0) startval = _store.Lists[listName].Count + startval;
                    if (stopval < 0) stopval = _store.Lists[listName].Count + stopval;
                    if (!_store.Lists.Keys.Contains(listName) || startval >= _store.Lists[listName].Count ||
                    startval > stopval)
                    {
                        await stream.WriteAsync(RespWriter.Encode("*0\r\n"));
                        continue;
                    }
                    if (stopval >= _store.Lists[listName].Count) stopval = _store.Lists[listName].Count - 1;
                    if (startval < 0) startval = 0;
                    var response = $"*{stopval - startval + 1}\r\n";
                    for (int i = startval; i <= stopval; i++)
                    {
                        response += $"${_store.Lists[listName][i].Length}\r\n{_store.Lists[listName][i]}\r\n";
                    }
                    await stream.WriteAsync(RespWriter.Encode(response));
                }
            }
                       else if (command.Equal("LPUSH"))
            {
                if (arguments.Length < 3)
                {
                    await stream.WriteAsync(RespWriter.Encode("-ERR invalid command argumentss\r\n"));
                    continue;
                }
                var listName = Decode(arguments[1]);
                var listLock = _store.ListLocks.GetOrAdd(listName, _ => new object());
                int resultLength;
                lock (listLock)
                {
                    var list = _store.Lists.GetOrAdd(listName, _ => new List<string>());
                    resultLength = list.Count;
                    for (int i = 2; i < arguments.Length; i++)
                    {
                        var value = Decode(arguments[i]);
                        resultLength++;
                        var delivered = false;
                        if (_store.WaitingClients.TryGetValue(listName, out var waiters))
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
                        if (!delivered) list.Insert(0, value);
                    }
                }
                await stream.WriteAsync(RespWriter.Encode($":{resultLength}\r\n"));
            }
            else if (command.Equal("LLEN"))
            {
                if (arguments.Length < 2)
                {
                    await stream.WriteAsync(RespWriter.Encode("-ERR invalid command argumentss\r\n"));
                    continue;
                }
                var listName = Decode(arguments[1]);
                if (!_store.Lists.Keys.Contains(listName))
                {
                    await stream.WriteAsync(RespWriter.Encode(":0\r\n"));
                    continue;
                }
                await stream.WriteAsync(RespWriter.Encode($":{_store.Lists[listName].Count}\r\n"));
            }
            else if (command.Equal("LPOP"))
            {
                if (arguments.Length < 2)
                {
                    await stream.WriteAsync(RespWriter.Encode("-ERR invalid command argumentss\r\n"));
                    continue;
                }
                int rangeval = 0;
                if (arguments.Length > 2)
                {
                    var range = Decode(arguments[2]);
                    if (!int.TryParse(range, out rangeval))
                    {
                        await stream.WriteAsync(RespWriter.Encode("-ERR invalid command argumentss\r\n"));
                        continue;
                    }
                }
                var listName = Decode(arguments[1]);
                if (!_store.Lists.Keys.Contains(listName) || _store.Lists[listName].Count == 0)
                {
                    await stream.WriteAsync(RespWriter.Encode("$-1\r\n"));
                    continue;
                }
                if (rangeval == 0)
                {
                    var value = _store.Lists[listName][0];
                    _store.Lists[listName].RemoveAt(0);
                    await stream.WriteAsync(RespWriter.Encode($"${value.Length}\r\n{value}\r\n"));
                    continue;
                }
                if (rangeval >= _store.Lists[listName].Count) rangeval = _store.Lists[listName].Count;
                var response = $"*{rangeval}\r\n";
                for (int i = 0; i < rangeval; i++)
                {
                    var value = _store.Lists[listName][0];
                    response += $"${value.Length}\r\n{value}\r\n";
                    _store.Lists[listName].RemoveAt(0);
                }
                await stream.WriteAsync(RespWriter.Encode(response));
            }
            else if (command.Equal("BLPOP"))
            {
                if (arguments.Length < 3)
                {
                    await stream.WriteAsync(RespWriter.Encode("-ERR invalid command argumentss\r\n"));
                    continue;
                }
                if (!double.TryParse(Decode(arguments[2]), out var timeout))
                {
                    await stream.WriteAsync(RespWriter.Encode("-ERR invalid command timout\r\n"));
                    continue;
                }
                var listName = Decode(arguments[1]);
                var listLock = _store.ListLocks.GetOrAdd(listName, _ => new object());
                var waiter = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
                string? immediateValue = null;
                lock (listLock)
                {
                    if (_store.Lists.TryGetValue(listName, out var list) && list.Count > 0)
                    {
                        immediateValue = list[0];
                        list.RemoveAt(0);
                    }
                    else
                    {
                        var waiters = _store.WaitingClients.GetOrAdd(listName, _ => new Queue<TaskCompletionSource<string>>());
                        waiters.Enqueue(waiter);
                    }
                }
                if (immediateValue is not null)
                {
                    await stream.WriteAsync(RespWriter.Encode($"*2\r\n${listName.Length}\r\n{listName}\r\n${immediateValue.Length}\r\n{immediateValue}\r\n"));
                    continue;
                }
                if (timeout == 0)
                {
                    var value = await waiter.Task;
                    await stream.WriteAsync(RespWriter.Encode($"*2\r\n${listName.Length}\r\n{listName}\r\n${value.Length}\r\n{value}\r\n"));
                }
                else if (timeout > 0)
                {
                    try
                    {
                        var value = await waiter.Task.WaitAsync(TimeSpan.FromSeconds(timeout));
                        await stream.WriteAsync(RespWriter.Encode($"*2\r\n${listName.Length}\r\n{listName}\r\n${value.Length}\r\n{value}\r\n"));

                    }
                    catch (TimeoutException)
                    {
                        string? deliveredValue = null;
                        lock (listLock)
                        {
                            if (waiter.Task.IsCompletedSuccessfully)
                            {
                                deliveredValue = waiter.Task.Result;
                            }
                            else if (_store.WaitingClients.TryGetValue(listName, out var queued))
                            {
                                var remaining = queued.Where(item => !ReferenceEquals(item, waiter)).ToArray();
                                queued.Clear();
                                foreach (var item in remaining) queued.Enqueue(item);
                            }
                        }
                        if (deliveredValue is not null)
                        {
                            await stream.WriteAsync(RespWriter.Encode($"*2\r\n${listName.Length}\r\n{listName}\r\n${deliveredValue.Length}\r\n{deliveredValue}\r\n"));
                            continue;
                        }
                        await stream.WriteAsync(RespWriter.Encode($"*-1\r\n"));
                    }
                }
            }
            else if (command.Equal("TYPE"))
            {
                if (arguments.Length < 2)
                {
                    await stream.WriteAsync(RespWriter.Encode("-ERR invalid command args"));
                    continue;
                }
                var key = Decode(arguments[1]);
                // string? value;
                if (_store.Keys.TryGetValue(key, out var value)) await stream.WriteAsync(RespWriter.Encode("+string\r\n"));
                else if (_store.Streams.TryGetValue(key, out var value2)) await stream.WriteAsync(RespWriter.Encode("+stream\r\n"));
                else await stream.WriteAsync(RespWriter.Encode("+none\r\n"));
            }
           else if (command.Equal("XADD"))
            {
                if (arguments.Length < 5 || arguments.Length % 2 == 0)
                {
                    await stream.WriteAsync(RespWriter.Encode("-ERR wrong number of arguments for 'xadd' command\r\n"));
                    continue;
                }
                var streamKey = Decode(arguments[1]);
                var requestedId = Decode(arguments[2]);
                var idParts = requestedId.Split('-');
                if (idParts.Length != 2 || !long.TryParse(idParts[0], out var millisecond) || millisecond < 0)
                {
                    await stream.WriteAsync(RespWriter.Encode("-ERR The ID specified in XADD must be greater than 0-0\r\n"));
                    continue;
                }

                var autoSequence = idParts[1].Equal("*");
                long sequence;
                if (autoSequence)
                {
                    sequence = millisecond == 0 ? 1 : 0;
                }
                else if (!long.TryParse(idParts[1], out sequence) || sequence < 0)
                {
                    await stream.WriteAsync(RespWriter.Encode("-ERR The ID specified in XADD must be greater than 0-0\r\n"));
                    continue;
                }

                if (millisecond == 0 && sequence == 0)
                {
                    await stream.WriteAsync(RespWriter.Encode("-ERR The ID specified in XADD must be greater than 0-0\r\n"));
                    continue;
                }
                var entries = _store.Streams.GetOrAdd(streamKey,
                 _ => new ConcurrentStack<StreamEntry>());
                if (entries.TryPeek(out var top))
                {
                    var topParts = top.Id.Split('-');
                    var topMilli = long.Parse(topParts[0]);
                    var topSequence = long.Parse(topParts[1]);
                    if (millisecond < topMilli || (millisecond == topMilli && !autoSequence && sequence <= topSequence))
                    {
                        await stream.WriteAsync(RespWriter.Encode("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"));
                        continue;
                    }
                    if (autoSequence && millisecond == topMilli) sequence = topSequence + 1;
                }
                var id = $"{millisecond}-{sequence}";
                var entry = new StreamEntry(id);
                for (int i = 3; i < arguments.Length; i += 2)
                {
                    var key = Decode(arguments[i]);
                    var value = Decode(arguments[i + 1]);
                    entry.Fields[key] = value;
                }
                entries.Push(entry);
                var response = RespWriter.Encode($"${entry.Id.Length}\r\n{entry.Id}\r\n");
                await stream.WriteAsync(response);
            }
        }

    }
}
