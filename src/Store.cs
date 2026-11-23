using System.Collections.Concurrent;

namespace My_Redis;

public class Store
{
    public ConcurrentDictionary<string, string> Keys { get; } = new();
    public ConcurrentDictionary<string, List<string>> Lists { get; } = new();
    public ConcurrentDictionary<string, Queue<TaskCompletionSource<string>>> WaitingClients { get; } = new();
    public ConcurrentDictionary<string, object> ListLocks { get; } = new();
    public ConcurrentDictionary<string, List<StreamEntry>> Streams { get; } = new();
    public ConcurrentDictionary<string, object> StreamLocks { get; } = new();
}
public class StreamEntry
{
    public string Id { get; set; }
    public Dictionary<string, string> Fields { get; set; }= new();
    public StreamEntry(string Id)
    {
        this.Id = Id;
    }
}
