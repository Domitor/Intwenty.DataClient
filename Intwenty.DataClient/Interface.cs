using Intwenty.DataClient.Model;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Text.Json;

namespace Intwenty.DataClient
{
    public interface IDataClient
    {
        DBMS Database { get; }
        void Open();
        void Close();
        void BeginTransaction();
        void CommitTransaction();
        void RollbackTransaction();
        void CreateTable<T>();
        bool TableExists<T>();
        bool TableExists(string tablename);
        bool ColumnExists(string tablename, string columnname);
        void RunCommand(string sql, bool isprocedure=false, IIntwentySqlParameter[] parameters=null);
        object GetScalarValue(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null);
        T GetEntity<T>(string id) where T : new();
        T GetEntity<T>(int id) where T : new();
        T GetEntity<T>(string sql, bool isprocedure) where T : new();
        T GetEntity<T>(string sql, bool isprocedure, IIntwentySqlParameter[] parameters = null) where T : new();
        List<T> GetEntities<T>() where T : new();
        List<T> GetEntities<T>(string sql, bool isprocedure=false) where T : new();
        List<T> GetEntities<T>(string sql, bool isprocedure, IIntwentySqlParameter[] parameters=null) where T : new();
        IJsonObjectResult GetJsonObject(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null);
        IJsonArrayResult GetJsonArray(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null);
        dynamic GetObject(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null);
        List<dynamic> GetObjects(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null);
        IResultSet GetResultSet(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null);
        DataTable GetDataTable(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null);
        int InsertEntity<T>(T entity);
        int InsertEntity(string json, string tablename);
        int InsertEntity(JsonElement json, string tablename);
        int UpdateEntity<T>(T entity);
        int UpdateEntity(string json, string tablename);
        int UpdateEntity(JsonElement json, string tablename);
        int DeleteEntity<T>(T entity);
        int DeleteEntities<T>(IEnumerable<T> entities);
        List<TypeMapItem> GetDbTypeMap();
        List<CommandMapItem> GetDbCommandMap();
    }


    public interface IIntwentyResultColumn
    {
        public string Name { get; }
        public bool IsNumeric { get; }
        public bool IsDateTime { get; }
    }

    public interface IIntwentySqlParameter
    {
        public string Name { get;  }
        public object Value { get; }
        public DbType DataType { get; }
        public ParameterDirection Direction { get; }
    }

    public interface IResultSet
    {
        string Name { get; set; }
        List<IResultSetRow> Rows { get;}
        public bool HasRows { get; }
        int? FirstRowGetAsInt(string name);
        string FirstRowGetAsString(string name);
        bool? FirstRowGetAsBool(string name);
        decimal? FirstRowGetAsDecimal(string name);
        DateTime? FirstRowGetAsDateTime(string name);
    }

    public interface IResultSetRow
    {
        List<IResultSetValue> Values { get; }
        int? GetAsInt(string name);
        string GetAsString(string name);
        bool? GetAsBool(string name);
        decimal? GetAsDecimal(string name);
        DateTime? GetAsDateTime(string name);
        void SetValue(string name, object value);
    }

    public interface IResultSetValue
    {
        string Name { get; set; }
        bool HasValue { get; }
        int? GetAsInt();
        string GetAsString();
        bool? GetAsBool();
        decimal? GetAsDecimal();
        DateTime? GetAsDateTime();
        void SetValue(object value);

    }

    public interface IJsonObjectResult
    {
        List<IResultSetValue> Values { get;  }

        string GetJsonString();

    }

    public interface IJsonArrayResult
    {
        int ObjectCount { get; }

        double Duration { get; }

        List<IJsonObjectResult> JsonObjects { get; }

        string GetJsonString();

    }

   
}
