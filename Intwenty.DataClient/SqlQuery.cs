

namespace Intwenty.DataClient
{
    public class SqlQuery : ISqlQuery
    {
        public bool IncludeExecutionInfo { get; set; }
        public string SqlStatement { get; set; }
        public string NumericIdColumn { get; set; }
    }
}
