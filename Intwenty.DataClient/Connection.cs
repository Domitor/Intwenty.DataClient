﻿using Intwenty.DataClient.Databases;
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

        /// <summary>
        /// If the table corresponding to the type, does not exists it will be created with the columns in the model.
        /// If the table corresponding to the type, already exists all columns that not already exists will be added.
        /// </summary>
        public void ModifyTable<T>()
        {
            InternalClient.ModifyTable<T>();
        }

        public string GetCreateTableSqlStatement<T>()
        {
            return InternalClient.GetCreateTableSqlStatement<T>();
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

        public async Task<List<T>> GetEntitiesAsync<T>() where T : new()
        {
            return await InternalClient.GetEntitiesAsync<T>();
        }

        public List<T> GetEntities<T>(string sql, bool isprocedure=false) where T : new()
        {
            return InternalClient.GetEntities<T>(sql, isprocedure);
        }

        public async Task<List<T>> GetEntitiesAsync<T>(string sql, bool isprocedure = false) where T : new()
        {
            return await InternalClient.GetEntitiesAsync<T>(sql, isprocedure);
        }

        public List<T> GetEntities<T>(string sql, bool isprocedure, IIntwentySqlParameter[] parameters = null) where T : new()
        {
            return InternalClient.GetEntities<T>(sql, isprocedure, parameters);
        }

        public async Task<List<T>> GetEntitiesAsync<T>(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null) where T : new()
        {
            return await InternalClient.GetEntitiesAsync<T>(sql, isprocedure, parameters);
        }

        public IJsonObjectResult GetJsonObject(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            return InternalClient.GetJsonObject(sql,isprocedure,parameters);
        }

        public async Task<IJsonObjectResult> GetJsonObjectAsync(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            return await InternalClient.GetJsonObjectAsync(sql, isprocedure, parameters);
        }

        public dynamic GetObject(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            return InternalClient.GetObject(sql, isprocedure, parameters);
        }
        public async Task<dynamic> GetObjectAsync(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            return await InternalClient.GetObjectAsync(sql, isprocedure, parameters);
        }
        public IJsonArrayResult GetJsonArray(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            return InternalClient.GetJsonArray(sql, isprocedure, parameters);
        }
        public async Task<IJsonArrayResult> GetJsonArrayAsync(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            return await InternalClient.GetJsonArrayAsync(sql, isprocedure, parameters);
        }
        public List<dynamic> GetObjects(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            return InternalClient.GetObjects(sql, isprocedure, parameters);
        }
        public async Task<List<dynamic>> GetObjectsAsync(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            return await InternalClient.GetObjectsAsync(sql, isprocedure, parameters);
        }


        public int InsertEntity<T>(T model)
        {
            return InternalClient.InsertEntity(model);
        }
        public async Task<int> InsertEntityAsync<T>(T model)
        {
            return await InternalClient.InsertEntityAsync(model);
        }
        public string GetInsertSqlStatement<T>(T model)
        {
            return InternalClient.GetInsertSqlStatement(model);
        }

        public int UpdateEntity<T>(T entity)
        {
            return InternalClient.UpdateEntity(entity);
        }
        public async Task<int> UpdateEntityAsync<T>(T entity)
        {
            return await InternalClient.UpdateEntityAsync(entity);
        }
        public string GetUpdateSqlStatement<T>(T model)
        {
            return InternalClient.GetUpdateSqlStatement(model);
        }

        public int DeleteEntity<T>(T entity)
        {
            return InternalClient.DeleteEntity(entity);
        }

        public async Task<int> DeleteEntityAsync<T>(T entity)
        {
            return await InternalClient.DeleteEntityAsync(entity);
        }

        public int DeleteEntities<T>(IEnumerable<T> entities)
        {
            return InternalClient.DeleteEntities(entities);
        }
        public async Task<int> DeleteEntitiesAsync<T>(IEnumerable<T> entities)
        {
            return await InternalClient.DeleteEntitiesAsync(entities);
        }

        public IResultSet GetResultSet(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            return InternalClient.GetResultSet(sql, isprocedure, parameters);
        }
        public async Task<IResultSet> GetResultSetAsync(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            return await InternalClient.GetResultSetAsync(sql, isprocedure, parameters);
        }

        public DataTable GetDataTable(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            return InternalClient.GetDataTable(sql,isprocedure, parameters);
        }

        public async Task<DataTable> GetDataTableAsync(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            return await InternalClient.GetDataTableAsync(sql, isprocedure, parameters);
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
