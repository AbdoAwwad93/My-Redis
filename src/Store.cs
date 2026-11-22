using System.Collections.Concurrent;

namespace My_Redis;

public class RedisStore
{
    public ConcurrentDictionary<string, string> Keys { get; } = new();
    public ConcurrentDictionary<string, List<string>> Lists { get; } = new();
    public ConcurrentDictionary<string, Queue<TaskCompletionSource<string>>> WaitingClients { get; } = new();
    public ConcurrentDictionary<string, ConcurrentDictionary<string, Dictionary<string, string>>> Streams { get; } = new();
}
