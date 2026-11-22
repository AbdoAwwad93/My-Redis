namespace My_Redis;
public static class StringCompare
{
    public static bool Equal(this string source, string target)
    {
        return string.Equals(source, target, StringComparison.OrdinalIgnoreCase);
    }
}
