using System.Net;
using My_Redis;

var store = new Store();
var server = new Server(store, IPAddress.Any, 6379);
await server.RunAsync();