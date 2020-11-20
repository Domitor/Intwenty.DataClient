﻿using Intwenty.DataClient.Databases;
using Intwenty.DataClient.Model;
using System.Collections.Generic;
using System.Data;
using System.Text.Json;

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

            if (Database == DBMS.SQLite)
                InternalClient = new Databases.SQLite.SQLiteClient(connectionstring);
            if (Database == DBMS.MySql)
                InternalClient = new Databases.MariaDb.MariaDbClient(connectionstring);
            if (Database == DBMS.MariaDB)
                InternalClient = new Databases.MariaDb.MariaDbClient(connectionstring);
            if (Database == DBMS.MSSqlServer)
                InternalClient = new Databases.SqlServer.SqlServerClient(connectionstring);
            if (Database == DBMS.PostgreSQL)
                InternalClient = new Databases.Postgres.PostgresClient(connectionstring);

        }

        public void BeginTransaction() 
        {
            InternalClient.BeginTransaction();
        }
        public void CommitTransaction()
        {
            InternalClient.CommitTransaction();
        }
        public void RollbackTransaction()
        {
            InternalClient.RollbackTransaction();
        }

        public void Open()
        {
            InternalClient.Open();
        }

        public void Close()
        {
            InternalClient.Close();
        }


        public void RunCommand(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            InternalClient.RunCommand(sql, isprocedure, parameters);
        }

        public object GetScalarValue(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            return InternalClient.GetScalarValue(sql, isprocedure, parameters);
        }

        public void CreateTable<T>()
        {
            InternalClient.CreateTable<T>();
        }

        public bool TableExists<T>()
        {
            return InternalClient.TableExists<T>();
        }

        public T GetEntity<T>(string id) where T : new()
        {
            return InternalClient.GetEntity<T>(id);
        }

        public T GetEntity<T>(int id) where T : new() 
        {
            return InternalClient.GetEntity<T>(id);
        }

        public T GetEntity<T>(string sql, bool isprocedure) where T : new()
        {
            return InternalClient.GetEntity<T>(sql, isprocedure);
        }

        public T GetEntity<T>(string sql, bool isprocedure, IIntwentySqlParameter[] parameters = null) where T : new()
        {
            return InternalClient.GetEntity<T>(sql, isprocedure, parameters);
        }

        public List<T> GetEntities<T>() where T : new()
        {
            return InternalClient.GetEntities<T>();
        }

        public List<T> GetEntities<T>(string sqlcommand, bool isStoredProcedure) where T : new()
        {
            return InternalClient.GetEntities<T>(sqlcommand, isStoredProcedure);
        }

        public List<T> GetEntities<T>(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null) where T : new()
        {
            return InternalClient.GetEntities<T>(sql, isprocedure, parameters);
        }

        public int InsertEntity<T>(T model)
        {
            return InternalClient.InsertEntity(model);
        }

        public int InsertEntity(string json, string tablename)
        {
            return InternalClient.InsertEntity(json, tablename);
        }

        public int InsertEntity(JsonElement json, string tablename)
        {
            return InternalClient.InsertEntity(json, tablename);
        }

        public IJsonObjectResult GetJSONObject(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null, IIntwentyResultColumn[] resultcolumns = null)
        {
            return InternalClient.GetJSONObject(sql,isprocedure,parameters,resultcolumns);
        }

        public bool TableExists(string tablename)
        {
            return InternalClient.TableExists(tablename);
        }

        public bool ColumnExists(string tablename, string columnname)
        {
            return InternalClient.ColumnExists(tablename, columnname);
        }

     

        public int UpdateEntity<T>(T entity)
        {
            return InternalClient.UpdateEntity(entity);
        }

        public int UpdateEntity(string json, string tablename)
        {
            return InternalClient.UpdateEntity(json, tablename);
        }

        public int UpdateEntity(JsonElement json, string tablename)
        {
            return InternalClient.UpdateEntity(json, tablename);
        }

        public int DeleteEntity<T>(T entity)
        {
            return InternalClient.DeleteEntity(entity);
        }

        public int DeleteEntities<T>(IEnumerable<T> entities)
        {
            return InternalClient.DeleteEntities(entities);
        }

        public IJsonArrayResult GetJSONArray(ISqlQuery sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null, IIntwentyResultColumn[] resultcolumns = null)
        {
            return InternalClient.GetJSONArray(sql,isprocedure,parameters,resultcolumns);
        }

        public IResultSet GetResultSet(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null, IIntwentyResultColumn[] resultcolumns = null)
        {
            return InternalClient.GetResultSet(sql, isprocedure, parameters, resultcolumns);
        }

        public DataTable GetDataTable(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null, IIntwentyResultColumn[] resultcolumns = null)
        {
            return InternalClient.GetDataTable(sql,isprocedure, parameters, resultcolumns);
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
