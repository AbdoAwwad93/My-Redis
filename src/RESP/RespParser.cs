using System.Text;
namespace My_Redis.RESP;
public class RespParser
{
    private readonly Stream _stream;
    public RespParser(Stream stream)
    {
        _stream = stream;
    }
    public async Task<byte[][]?> ReadCommandAsync(
        CancellationToken cancellationToken = default)
    {
        int prefix = await ReadByteAsync(cancellationToken);

        if (prefix == -1)
            return null;
        if (prefix != '*')
            throw new InvalidDataException("Expected RESP array.");

        string line = await ReadLineAsync(cancellationToken);

        if (!int.TryParse(line, out int count) || count < 0)
            throw new InvalidDataException("Invalid array length.");
        if (count > 1024)
            throw new InvalidDataException("Array too large.");

        var arguments = new byte[count][];
        for (int i = 0; i < count; i++)
        {
            int bulkPrefix = await ReadByteAsync(cancellationToken);

            if (bulkPrefix != '$')
                throw new InvalidDataException(
                    "Expected bulk string.");

            string lengthLine =
                await ReadLineAsync(cancellationToken);

            if (!int.TryParse(lengthLine, out int length))
                throw new InvalidDataException(
                    "Invalid bulk string length.");

            if (length < 0)
                throw new InvalidDataException(
                    "Null argument is not valid here.");

            if (length > 64 * 1024 * 1024)
                throw new InvalidDataException(
                    "Bulk string too large.");

            byte[] data = new byte[length];

            await ReadExactlyAsync(data, cancellationToken);

            await ReadCRLFAsync(cancellationToken);

            arguments[i] = data;
        }

        return arguments;
    }

    private async Task<int> ReadByteAsync(
        CancellationToken cancellationToken)
    {
        byte[] buffer = new byte[1];

        int bytesRead = await _stream.ReadAsync(
            buffer, cancellationToken);

        if (bytesRead == 0)
            return -1;

        return buffer[0];
    }

    private async Task<string> ReadLineAsync(
        CancellationToken cancellationToken)
    {
        using var memory = new MemoryStream();

        while (true)
        {
            int current = await ReadByteAsync(cancellationToken);

            if (current == -1)
                throw new EndOfStreamException();

            if (current == '\r')
            {
                int next = await ReadByteAsync(cancellationToken);

                if (next != '\n')
                    throw new InvalidDataException(
                        "Expected CRLF.");

                break;
            }

            memory.WriteByte((byte)current);

            if (memory.Length > 64 * 1024)
                throw new InvalidDataException(
                    "Protocol line too long.");
        }

        return Encoding.ASCII.GetString(
            memory.ToArray());
    }

    private async Task ReadExactlyAsync(
        byte[] buffer,
        CancellationToken cancellationToken)
    {
        int offset = 0;

        while (offset < buffer.Length)
        {
            int bytesRead = await _stream.ReadAsync(
                buffer.AsMemory(offset),
                cancellationToken);

            if (bytesRead == 0)
                throw new EndOfStreamException();

            offset += bytesRead;
        }
    }

    private async Task ReadCRLFAsync(
        CancellationToken cancellationToken)
    {
        int first = await ReadByteAsync(cancellationToken);
        int second = await ReadByteAsync(cancellationToken);

        if (first != '\r' || second != '\n')
            throw new InvalidDataException(
                "Expected CRLF after bulk string.");
    }
}
