using Intwenty.DataClient.Databases;
using Intwenty.DataClient.Model;
using System.Collections.Generic;
using System.Data;
using System.Text.Json;
using System.Threading.Tasks;

namespace Intwenty.DataClient
{
    public enum DBMS { MSSqlServer, MySql, MariaDB, PostgreSQL, SQLite };



    public class Connection : IDataClient
    {
        public DBMS Database { get; }

        public string ConnectionString { get; }

        private IDataClient InternalClient { get; }

        public Connection(DBMS database, string connectionstring)
        {
            Database = database;
            ConnectionString = connectionstring;

            var options = new DataClientOptions() { JsonNullValueHandling = JsonNullValueMode.Exclude };

            if (Database == DBMS.SQLite)
                InternalClient = new Databases.SQLite.SQLiteClient(connectionstring, options);
            if (Database == DBMS.MySql)
                InternalClient = new Databases.MariaDb.MariaDbClient(connectionstring, options);
            if (Database == DBMS.MariaDB)
                InternalClient = new Databases.MariaDb.MariaDbClient(connectionstring, options);
            if (Database == DBMS.MSSqlServer)
                InternalClient = new Databases.SqlServer.SqlServerClient(connectionstring, options);
            if (Database == DBMS.PostgreSQL)
                InternalClient = new Databases.Postgres.PostgresClient(connectionstring, options);

        }

        public Connection(DBMS database, string connectionstring, DataClientOptions options)
        {
            Database = database;
            ConnectionString = connectionstring;

            if (Database == DBMS.SQLite)
                InternalClient = new Databases.SQLite.SQLiteClient(connectionstring, options);
            if (Database == DBMS.MySql)
                InternalClient = new Databases.MariaDb.MariaDbClient(connectionstring, options);
            if (Database == DBMS.MariaDB)
                InternalClient = new Databases.MariaDb.MariaDbClient(connectionstring, options);
            if (Database == DBMS.MSSqlServer)
                InternalClient = new Databases.SqlServer.SqlServerClient(connectionstring, options);
            if (Database == DBMS.PostgreSQL)
                InternalClient = new Databases.Postgres.PostgresClient(connectionstring, options);

        }

        public void BeginTransaction() 
        {
            InternalClient.BeginTransaction();
        }

        public async Task BeginTransactionAsync()
        {
            await InternalClient.BeginTransactionAsync();
        }

        public void CommitTransaction()
        {
            InternalClient.CommitTransaction();
        }

        public async Task CommitTransactionAsync()
        {
            await InternalClient.CommitTransactionAsync();
        }

        public void RollbackTransaction()
        {
            InternalClient.RollbackTransaction();
        }

        public async Task RollbackTransactionAsync()
        {
            await InternalClient.RollbackTransactionAsync();
        }


        public void Open()
        {
            InternalClient.Open();
        }

        public async Task OpenAsync()
        {
            await InternalClient.OpenAsync();
        }

        public void Close()
        {
            InternalClient.Close();
        }

        public async Task CloseAsync()
        {
            await InternalClient.CloseAsync();
        }

        public void RunCommand(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            InternalClient.RunCommand(sql, isprocedure, parameters);
        }
        public async Task RunCommandAsync(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            await InternalClient.RunCommandAsync(sql, isprocedure, parameters);
        }

        public object GetScalarValue(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            return InternalClient.GetScalarValue(sql, isprocedure, parameters);
        }

        public async Task<object> GetScalarValueAsync(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            return await InternalClient.GetScalarValueAsync(sql, isprocedure, parameters);
        }

        public void CreateTable<T>()
        {
            InternalClient.CreateTable<T>();
        }

        public async Task CreateTableAsync<T>()
        {
            await InternalClient.CreateTableAsync<T>();
        }

        public bool TableExists<T>()
        {
            return InternalClient.TableExists<T>();
        }

        public async Task<bool> TableExistsAsync<T>()
        {
            return await InternalClient.TableExistsAsync<T>();
        }

        public bool TableExists(string tablename)
        {
            return InternalClient.TableExists(tablename);
        }

        public async Task<bool> TableExistsAsync(string tablename)
        {
            return await InternalClient.TableExistsAsync(tablename);
        }

        public bool ColumnExists(string tablename, string columnname)
        {
            return InternalClient.ColumnExists(tablename, columnname);
        }

        public async Task<bool> ColumnExistsAsync(string tablename, string columnname)
        {
            return await InternalClient.ColumnExistsAsync(tablename, columnname);
        }

        public int InsertEntity<T>(T model)
        {
            return InternalClient.InsertEntity(model);
        }

     

        public T GetEntity<T>(string id) where T : new()
        {
            return InternalClient.GetEntity<T>(id);
        }

        public async Task<T> GetEntityAsync<T>(string id) where T : new()
        {
            return await InternalClient.GetEntityAsync<T>(id);
        }

        public T GetEntity<T>(int id) where T : new()
        {
            return InternalClient.GetEntity<T>(id);
        }

        public async Task<T> GetEntityAsync<T>(int id) where T : new()
        {
            return await InternalClient.GetEntityAsync<T>(id);
        }

        public T GetEntity<T>(string sql, bool isprocedure) where T : new()
        {
            return InternalClient.GetEntity<T>(sql, isprocedure);
        }

        public async Task<T> GetEntityAsync<T>(string sql, bool isprocedure) where T : new()
        {
            return await InternalClient.GetEntityAsync<T>(sql, isprocedure);
        }

        public T GetEntity<T>(string sql, bool isprocedure, IIntwentySqlParameter[] parameters = null) where T : new()
        {
            return InternalClient.GetEntity<T>(sql, isprocedure, parameters);
        }

        public async Task<T> GetEntityAsync<T>(string sql, bool isprocedure, IIntwentySqlParameter[] parameters = null) where T : new()
        {
            return await InternalClient.GetEntityAsync<T>(sql, isprocedure, parameters);
        }

        public List<T> GetEntities<T>() where T : new()
        {
            return InternalClient.GetEntities<T>();
        }

        public List<T> GetEntities<T>(string sql, bool isprocedure=false) where T : new()
        {
            return InternalClient.GetEntities<T>(sql, isprocedure);
        }

        public List<T> GetEntities<T>(string sql, bool isprocedure, IIntwentySqlParameter[] parameters = null) where T : new()
        {
            return InternalClient.GetEntities<T>(sql, isprocedure, parameters);
        }

        public IJsonObjectResult GetJsonObject(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            return InternalClient.GetJsonObject(sql,isprocedure,parameters);
        }

        public dynamic GetObject(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            return InternalClient.GetObject(sql, isprocedure, parameters);
        }

        public IJsonArrayResult GetJsonArray(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            return InternalClient.GetJsonArray(sql, isprocedure, parameters);
        }

        public List<dynamic> GetObjects(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            return InternalClient.GetObjects(sql, isprocedure, parameters);
        }

      

        public int UpdateEntity<T>(T entity)
        {
            return InternalClient.UpdateEntity(entity);
        }

      

        public int DeleteEntity<T>(T entity)
        {
            return InternalClient.DeleteEntity(entity);
        }

        public int DeleteEntities<T>(IEnumerable<T> entities)
        {
            return InternalClient.DeleteEntities(entities);
        }

        public IResultSet GetResultSet(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            return InternalClient.GetResultSet(sql, isprocedure, parameters);
        }

        public DataTable GetDataTable(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            return InternalClient.GetDataTable(sql,isprocedure, parameters);
        }

        public List<TypeMapItem> GetDbTypeMap()
        {
            return TypeMap.GetTypeMap();
        }

        public List<CommandMapItem> GetDbCommandMap()
        {
            return CommandMap.GetCommandMap();
        }
    }


}
