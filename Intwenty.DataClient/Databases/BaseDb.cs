using Intwenty.DataClient.Model;
using Intwenty.DataClient.Reflection;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Intwenty.DataClient.Databases
{
    abstract class BaseDb : IDataClient
    {
        protected string ConnectionString { get; set; }

        protected bool IsInTransaction { get; set; }

        public abstract DBMS Database { get; }

        protected DataClientOptions Options { get; }

        public BaseDb(string connectionstring, DataClientOptions options) 
        {
            ConnectionString = connectionstring;
            Options = options;
        }
      
        protected abstract BaseSqlBuilder GetSqlBuilder();

        public List<TypeMapItem> GetDbTypeMap()
        {
            return TypeMap.GetTypeMap();
        }

        public List<CommandMapItem> GetDbCommandMap()
        {
            return CommandMap.GetCommandMap();
        }

        public void Open() { }
        public async Task OpenAsync() { await Task.CompletedTask;  }
        public abstract void Close();
        public abstract Task CloseAsync();
        protected abstract IDbCommand GetCommand();
        protected abstract Task<DbCommand> GetCommandAsync();
        protected abstract DbTransaction GetTransaction();
      

        public void BeginTransaction()
        {
            IsInTransaction = true;
        }
        public Task BeginTransactionAsync()
        {
            IsInTransaction = true;
            return Task.CompletedTask;
        }

        public virtual void CommitTransaction()
        {
            var transaction = GetTransaction();
            if (IsInTransaction && transaction != null)
            {
                IsInTransaction = false;
                transaction.Commit();
            }
           
        }

        public virtual async Task CommitTransactionAsync()
        {
            var transaction = GetTransaction();
            if (IsInTransaction && transaction != null)
            {
                IsInTransaction = false;
                await transaction.CommitAsync();
            }
        }

        public virtual void RollbackTransaction() 
        {
            var transaction = GetTransaction();
            if (IsInTransaction && transaction != null)
            {
                IsInTransaction = false;
                transaction.Rollback();
            }
        }

        public virtual async Task RollbackTransactionAsync()
        {
            var transaction = GetTransaction();
            if (IsInTransaction && transaction != null)
            {
                IsInTransaction = false;
                await transaction.RollbackAsync();
            }
        }


        public virtual void RunCommand(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
           
            using (var command = GetCommand())
            {
                command.CommandText = sql;
                if (isprocedure)
                    command.CommandType = CommandType.StoredProcedure;
                else
                    command.CommandType = CommandType.Text;

                AddCommandParameters(parameters, command);

                command.ExecuteNonQuery();

            }
            
        }

        public virtual async Task RunCommandAsync(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {

            using (var command = await GetCommandAsync())
            {
                command.CommandText = sql;
                if (isprocedure)
                    command.CommandType = CommandType.StoredProcedure;
                else
                    command.CommandType = CommandType.Text;

                AddCommandParameters(parameters, command);

                await command.ExecuteNonQueryAsync();

            }
        }

        public virtual object GetScalarValue(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            object res;


            using (var command = GetCommand())
            {
                command.CommandText = sql;
                if (isprocedure)
                    command.CommandType = CommandType.StoredProcedure;
                else
                    command.CommandType = CommandType.Text;

                AddCommandParameters(parameters, command);

                res = command.ExecuteScalar();
            }

 
            return res;
        }

        public virtual async Task<object> GetScalarValueAsync(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            object res;


            using (var command = await GetCommandAsync())
            {
                command.CommandText = sql;
                if (isprocedure)
                    command.CommandType = CommandType.StoredProcedure;
                else
                    command.CommandType = CommandType.Text;

                AddCommandParameters(parameters, command);

                res = await command.ExecuteScalarAsync();
            }


            return res;
        }

        public virtual dynamic GetObject(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            object result = null;

            var sqlstmt = GetSqlBuilder().GetModifiedSelectStatement(sql);

            using (var command = GetCommand())
            {
                command.CommandText = sqlstmt;
                if (isprocedure)
                    command.CommandType = CommandType.StoredProcedure;
                else
                    command.CommandType = CommandType.Text;

                AddCommandParameters(parameters, command);

                var reader = command.ExecuteReader();

                while (reader.Read())
                {
                    result = GetObject(reader);
                    break;
                }

                reader.Close();
                reader.Dispose();
            }

            return result;
        }

        public virtual async Task<dynamic> GetObjectAsync(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            object result = null;

            var sqlstmt = GetSqlBuilder().GetModifiedSelectStatement(sql);

            using (var command = await GetCommandAsync())
            {
                command.CommandText = sqlstmt;
                if (isprocedure)
                    command.CommandType = CommandType.StoredProcedure;
                else
                    command.CommandType = CommandType.Text;

                AddCommandParameters(parameters, command);

                var reader = await command.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {
                    result = GetObject(reader);
                    break;
                }

                await reader.CloseAsync();
                await reader.DisposeAsync();
            }

            return result;
        }

        public virtual IJsonObjectResult GetJsonObject(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            JsonObjectResult result = null;

            var sqlstmt = GetSqlBuilder().GetModifiedSelectStatement(sql);

            using (var command = GetCommand())
            {
                command.CommandText = sqlstmt;
                if (isprocedure)
                    command.CommandType = CommandType.StoredProcedure;
                else
                    command.CommandType = CommandType.Text;

                AddCommandParameters(parameters, command);

                var reader = command.ExecuteReader();

                while (reader.Read())
                {
                    result = GetJsonObjectResult(reader);
                    break;
                }

                reader.Close();
                reader.Dispose();

            }

            if (result == null)
                result = new JsonObjectResult();


            return result;
        }

        public virtual async Task<IJsonObjectResult> GetJsonObjectAsync(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            JsonObjectResult result = null;

            var sqlstmt = GetSqlBuilder().GetModifiedSelectStatement(sql);

            using (var command = await GetCommandAsync())
            {
                command.CommandText = sqlstmt;
                if (isprocedure)
                    command.CommandType = CommandType.StoredProcedure;
                else
                    command.CommandType = CommandType.Text;

                AddCommandParameters(parameters, command);

                var reader = await command.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {
                    result = GetJsonObjectResult(reader);
                    break;
                }

                await reader.CloseAsync();
                await reader.DisposeAsync();

            }

            if (result == null)
                result = new JsonObjectResult();


            return result;
        }

        public virtual List<dynamic> GetObjects(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            var res = new List<dynamic>();

            var sqlstmt = GetSqlBuilder().GetModifiedSelectStatement(sql);

            using (var command = GetCommand())
            {
                command.CommandText = sqlstmt;
                if (isprocedure)
                    command.CommandType = CommandType.StoredProcedure;
                else
                    command.CommandType = CommandType.Text;

                AddCommandParameters(parameters, command);

                var reader = command.ExecuteReader();

                while (reader.Read())
                {
                    res.Add(GetObject(reader));
                }

                reader.Close();
                reader.Dispose();
            }

            return res;
        }

        public virtual async Task<List<dynamic>> GetObjectsAsync(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            var res = new List<dynamic>();

            var sqlstmt = GetSqlBuilder().GetModifiedSelectStatement(sql);

            using (var command = await GetCommandAsync())
            {
                command.CommandText = sqlstmt;
                if (isprocedure)
                    command.CommandType = CommandType.StoredProcedure;
                else
                    command.CommandType = CommandType.Text;

                AddCommandParameters(parameters, command);

                var reader = await command.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {
                    res.Add(GetObject(reader));
                }

                await reader.CloseAsync();
                await reader.DisposeAsync();
            }

            return res;
        }

        public virtual IJsonArrayResult GetJsonArray(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            var start = DateTime.Now;
            var result = new JsonArrayResult();

            var sqlstmt = GetSqlBuilder().GetModifiedSelectStatement(sql);

            using (var command = GetCommand())
            {
                command.CommandText = sqlstmt;
                if (isprocedure)
                    command.CommandType = CommandType.StoredProcedure;
                else
                    command.CommandType = CommandType.Text;

                AddCommandParameters(parameters, command);

                var reader = command.ExecuteReader();

                while (reader.Read())
                {
                    result.JsonObjects.Add(GetJsonObjectResult(reader));

                }

                reader.Close();
                reader.Dispose();

            }
            result.Duration = DateTime.Now.Subtract(start).TotalMilliseconds;
            result.ObjectCount = result.JsonObjects.Count;


            return result;
        }

        public virtual async Task<IJsonArrayResult> GetJsonArrayAsync(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            var start = DateTime.Now;
            var result = new JsonArrayResult();

            var sqlstmt = GetSqlBuilder().GetModifiedSelectStatement(sql);

            using (var command = await GetCommandAsync())
            {
                command.CommandText = sqlstmt;
                if (isprocedure)
                    command.CommandType = CommandType.StoredProcedure;
                else
                    command.CommandType = CommandType.Text;

                AddCommandParameters(parameters, command);

                var reader = await command.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {
                    result.JsonObjects.Add(GetJsonObjectResult(reader));

                }

                await reader.CloseAsync();
                await reader.DisposeAsync();

            }
            result.Duration = DateTime.Now.Subtract(start).TotalMilliseconds;
            result.ObjectCount = result.JsonObjects.Count;


            return result;
        }

        public virtual IResultSet GetResultSet(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            var res = new ResultSet();

            var sqlstmt = GetSqlBuilder().GetModifiedSelectStatement(sql);

            using (var command = GetCommand())
            {
                command.CommandText = sqlstmt;
                if (isprocedure)
                    command.CommandType = CommandType.StoredProcedure;
                else
                    command.CommandType = CommandType.Text;

                AddCommandParameters(parameters, command);

                var reader = command.ExecuteReader();

                while (reader.Read())
                {

                    var row = new ResultSetRow();
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        if (reader.IsDBNull(i) && Options.JsonNullValueHandling == JsonNullValueMode.Exclude)
                            continue;

                        if (reader.IsDBNull(i))
                            row.SetValue(reader.GetName(i), null);
                        else
                            row.SetValue(reader.GetName(i), reader.GetValue(i));

                    }
                    res.Rows.Add(row);

                }

                reader.Close();
                reader.Dispose();
            }


            return res;
        }

        public virtual async Task<IResultSet> GetResultSetAsync(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            var res = new ResultSet();

            var sqlstmt = GetSqlBuilder().GetModifiedSelectStatement(sql);

            using (var command = await GetCommandAsync())
            {
                command.CommandText = sqlstmt;
                if (isprocedure)
                    command.CommandType = CommandType.StoredProcedure;
                else
                    command.CommandType = CommandType.Text;

                AddCommandParameters(parameters, command);

                var reader = await command.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {

                    var row = new ResultSetRow();
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        if (reader.IsDBNull(i) && Options.JsonNullValueHandling == JsonNullValueMode.Exclude)
                            continue;

                        if (reader.IsDBNull(i))
                            row.SetValue(reader.GetName(i), null);
                        else
                            row.SetValue(reader.GetName(i), reader.GetValue(i));

                    }
                    res.Rows.Add(row);

                }

                await reader.CloseAsync();
                await reader.DisposeAsync();
            }


            return res;
        }

        public virtual DataTable GetDataTable(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            var table = new DataTable();

            var sqlstmt = GetSqlBuilder().GetModifiedSelectStatement(sql);

            using (var command = GetCommand())
            {
                command.CommandText = sqlstmt;
                if (isprocedure)
                    command.CommandType = CommandType.StoredProcedure;
                else
                    command.CommandType = CommandType.Text;

                AddCommandParameters(parameters, command);

                var reader = command.ExecuteReader();

                table.Load(reader);

                reader.Close();
                reader.Dispose();

            }

            return table;
        }

        public virtual async Task<DataTable> GetDataTableAsync(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            var table = new DataTable();

            var sqlstmt = GetSqlBuilder().GetModifiedSelectStatement(sql);

            using (var command = await GetCommandAsync())
            {
                command.CommandText = sqlstmt;
                if (isprocedure)
                    command.CommandType = CommandType.StoredProcedure;
                else
                    command.CommandType = CommandType.Text;

                AddCommandParameters(parameters, command);

                var reader = await command.ExecuteReaderAsync();

                table.Load(reader);

                await reader.CloseAsync();
                await reader.DisposeAsync();

            }

            return table;
        }

        public virtual void CreateTable<T>()
        {
            var info = TypeDataHandler.GetDbTableDefinition<T>();

            if (TableExists<T>())
                return;

            using (var command = GetCommand())
            {
                command.CommandText = GetSqlBuilder().GetCreateTableSql(info);
                command.CommandType = CommandType.Text;
                command.ExecuteNonQuery();
            }

            foreach (var index in info.Indexes)
            {
                using (var command = GetCommand())
                {
                    command.CommandText = GetSqlBuilder().GetCreateIndexSql(index);
                    command.CommandType = CommandType.Text;
                    command.ExecuteNonQuery();
                }
            }

          
        }

        public virtual async Task CreateTableAsync<T>()
        {
            var info = TypeDataHandler.GetDbTableDefinition<T>();

            if (TableExists<T>())
                return;

            using (var command = await GetCommandAsync())
            {
                command.CommandText = GetSqlBuilder().GetCreateTableSql(info);
                command.CommandType = CommandType.Text;
                await command.ExecuteNonQueryAsync();
            }

            foreach (var index in info.Indexes)
            {
                using (var command = await GetCommandAsync())
                {
                    command.CommandText = GetSqlBuilder().GetCreateIndexSql(index);
                    command.CommandType = CommandType.Text;
                    await command.ExecuteNonQueryAsync();
                }
            }


        }

        public virtual string GetCreateTableSqlStatement<T>()
        {
            var info = TypeDataHandler.GetDbTableDefinition<T>();
            return GetSqlBuilder().GetCreateTableSql(info);
        }

        public virtual bool TableExists<T>()
        {
            var info = TypeDataHandler.GetDbTableDefinition<T>();
            return TableExists(info.Name);
        }

        public virtual async Task<bool> TableExistsAsync<T>()
        {
            var info = TypeDataHandler.GetDbTableDefinition<T>();
            return await TableExistsAsync(info.Name);
        }

        public virtual bool TableExists(string tablename)
        {
            try
            {
                using (var command = GetCommand())
                {
                    command.CommandText = string.Format("SELECT 1 FROM {0}", tablename);
                    command.CommandType = CommandType.Text;
                    command.ExecuteScalar();
                }
                return true;
            }
            catch { }
           
            return false;
        }

        public virtual async Task<bool> TableExistsAsync(string tablename)
        {
            try
            {
                using (var command = await GetCommandAsync())
                {
                    command.CommandText = string.Format("SELECT 1 FROM {0}", tablename);
                    command.CommandType = CommandType.Text;
                    await command.ExecuteScalarAsync();
                }
                return true;
            }
            catch { }

            return false;
        }

        public virtual bool ColumnExists(string tablename, string columnname)
        {
            try
            {
                using (var checkcommand = GetCommand())
                {
                    checkcommand.CommandText = string.Format("SELECT {0} FROM {1} WHERE 1=2", columnname, tablename);
                    checkcommand.CommandType = CommandType.Text;
                    checkcommand.ExecuteScalar();
                }
                return true;
            }
            catch { }
          
            return false;
        }

        public virtual async Task<bool> ColumnExistsAsync(string tablename, string columnname)
        {
            try
            {
                using (var checkcommand = await GetCommandAsync())
                {
                    checkcommand.CommandText = string.Format("SELECT {0} FROM {1} WHERE 1=2", columnname, tablename);
                    checkcommand.CommandType = CommandType.Text;
                    await checkcommand.ExecuteScalarAsync();
                }
                return true;
            }
            catch { }

            return false;
        }

        public virtual T GetEntity<T>(int id) where T : new()
        {
            return GetEntityById<T>(id);
        }

        public virtual async Task<T> GetEntityAsync<T>(int id) where T : new()
        {
            return await GetEntityByIdAsync<T>(id);
        }

        public virtual T GetEntity<T>(string id) where T : new()
        {
            return GetEntityById<T>(id);
        }

        public virtual async Task<T> GetEntityAsync<T>(string id) where T : new()
        {
            return await GetEntityByIdAsync<T>(id);
        }

        protected virtual T GetEntityById<T>(object id) where T : new()
        {
            var res = default(T);
            var info = TypeDataHandler.GetDbTableDefinition<T>();

            if (info.PrimaryKeyColumnNamesList.Count == 0)
                throw new InvalidOperationException("No primary key column found");
            if (info.PrimaryKeyColumnNamesList.Count > 1)
                throw new InvalidOperationException(string.Format("The table {0} uses a composite primary key", info.Name));


            using (var command = GetCommand())
            {
                command.CommandText = string.Format("SELECT * FROM {0} WHERE {1}=@P1", info.Name, info.PrimaryKeyColumnNamesList[0]);
                command.CommandType = CommandType.Text;

                AddCommandParameters(new IIntwentySqlParameter[] { new IntwentySqlParameter("@P1", id) }, command);

                var reader = command.ExecuteReader();

                while (reader.Read())
                {
                    res = new T();
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        if (reader.IsDBNull(i))
                            continue;

                        var col = info.Columns.Find(p => p.Name.ToUpper() == reader.GetName(i).ToUpper());
                        if (col != null)
                            SetPropertyValues(reader, new KeyValuePair<int, IntwentyDbColumnDefinition>(i, col), res);
                    }

                    break;

                }

                reader.Close();
                reader.Dispose();
            }

            return res;
        }

        protected virtual async Task<T> GetEntityByIdAsync<T>(object id) where T : new()
        {
            var res = default(T);
            var info = TypeDataHandler.GetDbTableDefinition<T>();

            if (info.PrimaryKeyColumnNamesList.Count == 0)
                throw new InvalidOperationException("No primary key column found");
            if (info.PrimaryKeyColumnNamesList.Count > 1)
                throw new InvalidOperationException(string.Format("The table {0} uses a composite primary key", info.Name));


            using (var command = await GetCommandAsync())
            {
                command.CommandText = string.Format("SELECT * FROM {0} WHERE {1}=@P1", info.Name, info.PrimaryKeyColumnNamesList[0]);
                command.CommandType = CommandType.Text;

                AddCommandParameters(new IIntwentySqlParameter[] { new IntwentySqlParameter("@P1", id) }, command);

                var reader = await command.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {
                    res = new T();
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        if (reader.IsDBNull(i))
                            continue;

                        var col = info.Columns.Find(p => p.Name.ToUpper() == reader.GetName(i).ToUpper());
                        if (col != null)
                            SetPropertyValues(reader, new KeyValuePair<int, IntwentyDbColumnDefinition>(i, col), res);
                    }

                    break;

                }

                await reader.CloseAsync();
                await reader.DisposeAsync();
            }

            return res;
        }

        public virtual T GetEntity<T>(string sql, bool isprocedure) where T : new()
        {
            return GetEntity<T>(sql, isprocedure, null);
        }

        public virtual async Task<T> GetEntityAsync<T>(string sql, bool isprocedure) where T : new()
        {
            return await GetEntityAsync<T>(sql, isprocedure, null);
        }

        public virtual T GetEntity<T>(string sql, bool isprocedure, IIntwentySqlParameter[] parameters = null) where T : new()
        {

            var res = default(T);
            var info = TypeDataHandler.GetDbTableDefinition<T>();

            var sqlstmt = GetSqlBuilder().GetModifiedSelectStatement(sql);

            using (var command = GetCommand())
            { 
                command.CommandText = sqlstmt;
                if (isprocedure)
                    command.CommandType = CommandType.StoredProcedure;
                else
                    command.CommandType = CommandType.Text;

                AddCommandParameters(parameters, command);

                var reader = command.ExecuteReader();

                while (reader.Read())
                {
                    res = new T();
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        if (reader.IsDBNull(i))
                            continue;

                        var col = info.Columns.Find(p => p.Name.ToUpper() == reader.GetName(i).ToUpper());
                        if (col!=null)
                            SetPropertyValues(reader, new KeyValuePair<int, IntwentyDbColumnDefinition>(i, col), res);
                    }

                    break;
                }

                reader.Close();
                reader.Dispose();
            }

            return res;
        }

        public virtual async Task<T> GetEntityAsync<T>(string sql, bool isprocedure, IIntwentySqlParameter[] parameters = null) where T : new()
        {

            var res = default(T);
            var info = TypeDataHandler.GetDbTableDefinition<T>();

            var sqlstmt = GetSqlBuilder().GetModifiedSelectStatement(sql);

            using (var command = await GetCommandAsync())
            {
                command.CommandText = sqlstmt;
                if (isprocedure)
                    command.CommandType = CommandType.StoredProcedure;
                else
                    command.CommandType = CommandType.Text;

                AddCommandParameters(parameters, command);

                var reader = await command.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {
                    res = new T();
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        if (reader.IsDBNull(i))
                            continue;

                        var col = info.Columns.Find(p => p.Name.ToUpper() == reader.GetName(i).ToUpper());
                        if (col != null)
                            SetPropertyValues(reader, new KeyValuePair<int, IntwentyDbColumnDefinition>(i, col), res);
                    }

                    break;
                }

                await reader.CloseAsync();
                await reader.DisposeAsync();
            }

            return res;
        }



        public virtual List<T> GetEntities<T>() where T : new()
        {
            var res = new List<T>();
            var info = TypeDataHandler.GetDbTableDefinition<T>();
            var readercolumns = new Dictionary<int,IntwentyDbColumnDefinition>();

            using (var command = GetCommand())
            {
                command.CommandText = string.Format("SELECT * FROM {0}", info.Name);
                command.CommandType = CommandType.Text;

                var reader = command.ExecuteReader();

                while (reader.Read())
                {
                    if (readercolumns.Count == 0)
                    {
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            var col = info.Columns.Find(p => p.Name.ToUpper() == reader.GetName(i).ToUpper());
                            if (col != null)
                                readercolumns.Add(i,col);
                        }
                    }

                    var m = new T();
   
                    foreach (var col in readercolumns)
                    {
                        if (reader.IsDBNull(col.Key))
                            continue;

                         SetPropertyValues(reader, col, m);

                    }
                    
                    res.Add(m);
                }

                reader.Close();
                reader.Dispose();
            }
  

            return res;
        }

        public virtual async Task<List<T>> GetEntitiesAsync<T>() where T : new()
        {
            var res = new List<T>();
            var info = TypeDataHandler.GetDbTableDefinition<T>();
            var readercolumns = new Dictionary<int, IntwentyDbColumnDefinition>();

            using (var command = await GetCommandAsync())
            {
                command.CommandText = string.Format("SELECT * FROM {0}", info.Name);
                command.CommandType = CommandType.Text;

                var reader = await command.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {
                    if (readercolumns.Count == 0)
                    {
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            var col = info.Columns.Find(p => p.Name.ToUpper() == reader.GetName(i).ToUpper());
                            if (col != null)
                                readercolumns.Add(i, col);
                        }
                    }

                    var m = new T();

                    foreach (var col in readercolumns)
                    {
                        if (reader.IsDBNull(col.Key))
                            continue;

                        SetPropertyValues(reader, col, m);

                    }

                    res.Add(m);
                }

                await reader.CloseAsync();
                await reader.DisposeAsync();
            }


            return res;
        }

        public virtual List<T> GetEntities<T>(string sql, bool isprocedure = false) where T : new() 
        {
            return GetEntities<T>(sql, isprocedure, null);
        }

        public virtual async Task<List<T>> GetEntitiesAsync<T>(string sql, bool isprocedure = false) where T : new()
        {
            return await GetEntitiesAsync<T>(sql, isprocedure, null);
        }

        public virtual List<T> GetEntities<T>(string sql, bool isprocedure, IIntwentySqlParameter[] parameters = null) where T : new()
        {

            var res = new List<T>();
            var info = TypeDataHandler.GetDbTableDefinition<T>();
            var readercolumns = new Dictionary<int, IntwentyDbColumnDefinition>();

            var sqlstmt = GetSqlBuilder().GetModifiedSelectStatement(sql);

            using (var command = GetCommand())
            {
                command.CommandText = sqlstmt;
                if (isprocedure)
                    command.CommandType = CommandType.StoredProcedure;
                else
                    command.CommandType = CommandType.Text;

                AddCommandParameters(parameters, command);

                var reader = command.ExecuteReader();

                while (reader.Read())
                {

                    if (readercolumns.Count == 0)
                    {
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            var col = info.Columns.Find(p => p.Name.ToUpper() == reader.GetName(i).ToUpper());
                            if (col != null)
                                readercolumns.Add(i,col);
                            
                        }
                    }

                    var m = new T();
                    foreach (var col in readercolumns)
                    {
                        if (reader.IsDBNull(col.Key))
                            continue;

                        SetPropertyValues(reader, col, m);

                    }
                    res.Add(m);
                }

                reader.Close();
                reader.Dispose();
            }

            return res;
        }

        public virtual async Task<List<T>> GetEntitiesAsync<T>(string sql, bool isprocedure, IIntwentySqlParameter[] parameters = null) where T : new()
        {

            var res = new List<T>();
            var info = TypeDataHandler.GetDbTableDefinition<T>();
            var readercolumns = new Dictionary<int, IntwentyDbColumnDefinition>();

            var sqlstmt = GetSqlBuilder().GetModifiedSelectStatement(sql);

            using (var command = await GetCommandAsync())
            {
                command.CommandText = sqlstmt;
                if (isprocedure)
                    command.CommandType = CommandType.StoredProcedure;
                else
                    command.CommandType = CommandType.Text;

                AddCommandParameters(parameters, command);

                var reader = await command.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {

                    if (readercolumns.Count == 0)
                    {
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            var col = info.Columns.Find(p => p.Name.ToUpper() == reader.GetName(i).ToUpper());
                            if (col != null)
                                readercolumns.Add(i, col);

                        }
                    }

                    var m = new T();
                    foreach (var col in readercolumns)
                    {
                        if (reader.IsDBNull(col.Key))
                            continue;

                        SetPropertyValues(reader, col, m);

                    }
                    res.Add(m);
                }

                await reader.CloseAsync();
                await reader.DisposeAsync();
            }

            return res;
        }


        public virtual int InsertEntity<T>(T entity)
        {
            if (entity == null)
                return 0;

            var info = TypeDataHandler.GetDbTableDefinition<T>();
            var parameters = new List<IntwentySqlParameter>();
            int res;

            using (var command = GetCommand())
            {
                command.CommandText = GetSqlBuilder().GetInsertSql(info, entity, parameters);
                command.CommandType = CommandType.Text;

                AddCommandParameters(parameters.ToArray(), command);

                res = command.ExecuteNonQuery();

                InferAutoIncrementalValue(info, parameters, entity, command);

            }

            return res;
        }

        public virtual string GetInsertSqlStatement<T>(T entity)
        {
            if (entity == null)
                return "";

            var info = TypeDataHandler.GetDbTableDefinition<T>();

            var separator = "";
            var query = new StringBuilder(string.Format("INSERT INTO {0} (", info.Name));
            var values = new StringBuilder(" VALUES (");

            foreach (var col in info.Columns.OrderBy(p => p.Index))
            {
                if (col.IsAutoIncremental)
                    continue;

                var value = col.Property.GetValue(entity);
                var strvalue = "";

                if (value == null)
                {
                    continue;
                }
                else
                {
                    if (col.IsString)
                        strvalue = "'" + Convert.ToString(value) + "'";
                    else if (col.IsDateTime)
                        strvalue = "'" + Convert.ToDateTime(value).ToString("yyyy-MM-dd HH:mm:ss") + "'";
                    else if (col.IsDateTimeOffset)
                        strvalue = "'" + ((DateTimeOffset)value).DateTime.ToString("yyyy-MM-dd HH:mm:ss") + "'";
                    else if (col.IsBoolean)
                    {
                        if (Convert.ToBoolean(value))
                            strvalue = "1";
                        else
                            strvalue = "0";
                    }
                    else
                        strvalue = Convert.ToString(value).Replace(",", ".");
                }
              
                if (Database == DBMS.MariaDB || Database == DBMS.MySql)
                    query.Append(string.Format("{0}`{1}`", separator, col.Name));
                else
                    query.Append(string.Format("{0}{1}", separator, col.Name));

                values.Append(string.Format("{0}{1}", separator, strvalue));
                separator = ",";

            }
            query.Append(") ");
            values.Append(")");

            return query.Append(values).ToString();

        }

        public virtual async Task<int> InsertEntityAsync<T>(T entity)
        {
            if (entity == null)
                return 0;

            var info = TypeDataHandler.GetDbTableDefinition<T>();
            var parameters = new List<IntwentySqlParameter>();
            int res;

            using (var command = await GetCommandAsync())
            {
                command.CommandText = GetSqlBuilder().GetInsertSql(info, entity, parameters);
                command.CommandType = CommandType.Text;

                AddCommandParameters(parameters.ToArray(), command);

                res = await command.ExecuteNonQueryAsync();

                InferAutoIncrementalValue(info, parameters, entity, command);

            }

            return res;
        }


        public virtual int UpdateEntity<T>(T entity)
        {
            if (entity == null)
                return 0;

            var info = TypeDataHandler.GetDbTableDefinition<T>();
            var parameters = new List<IntwentySqlParameter>();
            var keyparameters = new List<IntwentySqlParameter>();
            int res;

            var sql = GetSqlBuilder().GetUpdateSql(info, entity, parameters,keyparameters);
            if (keyparameters.Count == 0)
                throw new InvalidOperationException("Can't update a table without 'Primary Key' or an 'Auto Increment' column, please use annotations.");

            using (var command = GetCommand())
            {
                command.CommandText = sql;
                command.CommandType = CommandType.Text;

                AddCommandParameters(keyparameters.ToArray(), command);
                AddCommandParameters(parameters.ToArray(), command);

                res = command.ExecuteNonQuery();
            }
 
            return res;
        }

        public virtual async Task<int> UpdateEntityAsync<T>(T entity)
        {
            if (entity == null)
                return 0;

            var info = TypeDataHandler.GetDbTableDefinition<T>();
            var parameters = new List<IntwentySqlParameter>();
            var keyparameters = new List<IntwentySqlParameter>();
            int res;

            var sql = GetSqlBuilder().GetUpdateSql(info, entity, parameters, keyparameters);
            if (keyparameters.Count == 0)
                throw new InvalidOperationException("Can't update a table without 'Primary Key' or an 'Auto Increment' column, please use annotations.");

            using (var command = await GetCommandAsync())
            {
                command.CommandText = sql;
                command.CommandType = CommandType.Text;

                AddCommandParameters(keyparameters.ToArray(), command);
                AddCommandParameters(parameters.ToArray(), command);

                res = await command.ExecuteNonQueryAsync();
            }

            return res;
        }


        public virtual string GetUpdateSqlStatement<T>(T entity)
        {

            if (entity == null)
                return "";

            var info = TypeDataHandler.GetDbTableDefinition<T>();
            var keyparameters = new List<IntwentySqlParameter>();
            var separator = "";
            var query = new StringBuilder(string.Format("UPDATE {0} SET ", info.Name));

            foreach (var col in info.Columns)
            {

                var value = col.Property.GetValue(entity);

                if (!keyparameters.Exists(p => p.Name == col.Name) && value != null && col.IsAutoIncremental)
                    keyparameters.Add(new IntwentySqlParameter() { Name = col.Name, Value = value, DataType = DbType.Int32 });
                else if (!keyparameters.Exists(p => p.Name == col.Name) && value != null && col.IsPrimaryKeyColumn && (col.IsString))
                    keyparameters.Add(new IntwentySqlParameter() { Name = col.Name, Value = value, DataType= DbType.String });
                else if (!keyparameters.Exists(p => p.Name == col.Name) && value != null && col.IsPrimaryKeyColumn && (col.IsBoolean))
                    keyparameters.Add(new IntwentySqlParameter() { Name = col.Name, Value = value, DataType = DbType.Boolean });
                else if (!keyparameters.Exists(p => p.Name == col.Name) && value != null && col.IsPrimaryKeyColumn && (col.IsDateTime || col.IsDateTimeOffset))
                    keyparameters.Add(new IntwentySqlParameter() { Name = col.Name, Value = value, DataType = DbType.DateTime });
                else if (!keyparameters.Exists(p => p.Name == col.Name) && value != null && col.IsPrimaryKeyColumn)
                    keyparameters.Add(new IntwentySqlParameter() { Name = col.Name, Value = value, DataType = DbType.Int32 });

                if (keyparameters.Exists(p => p.Name == col.Name))
                    continue;

                var strvalue = "";

                if (value == null)
                {
                    continue;
                }
                else
                {
                    if (col.IsString)
                        strvalue = "'" + Convert.ToString(value) + "'";
                    else if (col.IsDateTime)
                        strvalue = "'" + Convert.ToDateTime(value).ToString("yyyy-MM-dd HH:mm:ss") + "'";
                    else if (col.IsDateTimeOffset)
                        strvalue = "'" + ((DateTimeOffset)value).DateTime.ToString("yyyy-MM-dd HH:mm:ss") + "'";
                    else if (col.IsBoolean)
                    {
                        if (Convert.ToBoolean(value))
                            strvalue = "1";
                        else
                            strvalue = "0";
                    }
                    else
                        strvalue = Convert.ToString(value).Replace(",", ".");
                }

                if (Database== DBMS.MariaDB || Database == DBMS.MySql)
                    query.Append(separator + string.Format("`{0}`={1}", col.Name, strvalue));
                else
                    query.Append(separator + string.Format("`{0}`={1}", col.Name, strvalue));

                separator = ", ";
            }

            if (keyparameters.Count == 0)
                return "";


            query.Append(" WHERE ");
            var wheresep = "";
            foreach (var p in keyparameters)
            {
                var strvalue = "";

                if (p.Value == null)
                {
                    continue;
                }
                else
                {
                    if (p.DataType == DbType.String)
                        strvalue = "'" + Convert.ToString(p.Value) + "'";
                    else if (p.DataType == DbType.DateTime)
                        strvalue = "'" + Convert.ToDateTime(p.Value).ToString("yyyy-MM-dd HH:mm:ss") + "'";
                    else if (p.DataType == DbType.Boolean)
                    {
                        if (Convert.ToBoolean(p.Value))
                            strvalue = "1";
                        else
                            strvalue = "0";
                    }
                    else
                        strvalue = Convert.ToString(p.Value).Replace(",", ".");
                }


                if (Database == DBMS.MariaDB || Database == DBMS.MySql)
                    query.Append(wheresep + string.Format("`{0}`={1}", p.Name,strvalue));
                else
                    query.Append(wheresep + string.Format("{0}={1}", p.Name, strvalue));

                wheresep = " AND ";
            }

            return query.ToString();


        }

        public virtual int DeleteEntities<T>(IEnumerable<T> entities)
        {
            if (entities == null)
                return 0;

            var res = 0;
            foreach (var t in entities)
            {
                res += DeleteEntity(t);
            }
            return res;
        }

        public virtual async Task<int> DeleteEntitiesAsync<T>(IEnumerable<T> entities)
        {
            if (entities == null)
                return 0;

            var res = 0;
            foreach (var t in entities)
            {
                res += await DeleteEntityAsync(t);
            }
            return res;
        }

        public virtual int DeleteEntity<T>(T entity)
        {
            if (entity == null)
                return 0;

            int res;
            var info = TypeDataHandler.GetDbTableDefinition<T>();
            var parameters = new List<IntwentySqlParameter>();

            var sql = GetSqlBuilder().GetDeleteSql(info, entity, parameters);
            if (parameters.Count == 0)
                throw new InvalidOperationException("Can't delete rows in a table without 'Primary Key' or an 'Auto Increment' column, please use annotations.");

            using (var command = GetCommand())
            {
                command.CommandText = sql;
                command.CommandType = CommandType.Text;

                AddCommandParameters(parameters.ToArray(), command);

                res = command.ExecuteNonQuery();
            }
    

            return res;
        }

        public virtual async Task<int> DeleteEntityAsync<T>(T entity)
        {
            if (entity == null)
                return 0;

            int res;
            var info = TypeDataHandler.GetDbTableDefinition<T>();
            var parameters = new List<IntwentySqlParameter>();

            var sql = GetSqlBuilder().GetDeleteSql(info, entity, parameters);
            if (parameters.Count == 0)
                throw new InvalidOperationException("Can't delete rows in a table without 'Primary Key' or an 'Auto Increment' column, please use annotations.");

            using (var command = await GetCommandAsync())
            {
                command.CommandText = sql;
                command.CommandType = CommandType.Text;

                AddCommandParameters(parameters.ToArray(), command);

                res = await command.ExecuteNonQueryAsync();
            }


            return res;
        }



        protected virtual void SetPropertyValues<T>(IDataReader reader, KeyValuePair<int,IntwentyDbColumnDefinition> column, T instance)
        {

            if (column.Value.IsInt32)
                column.Value.Property.SetValue(instance, reader.GetInt32(column.Key), null);
            else if (column.Value.IsBoolean)
                column.Value.Property.SetValue(instance, reader.GetBoolean(column.Key), null);
            else if (column.Value.IsDecimal)
                column.Value.Property.SetValue(instance, reader.GetDecimal(column.Key), null);
            else if (column.Value.IsSingle)
                column.Value.Property.SetValue(instance, reader.GetFloat(column.Key), null);
            else if (column.Value.IsDouble)
                column.Value.Property.SetValue(instance, reader.GetDouble(column.Key), null);
            else if (column.Value.IsDateTime)
                column.Value.Property.SetValue(instance, reader.GetDateTime(column.Key), null);
            else if (column.Value.IsDateTimeOffset)
                column.Value.Property.SetValue(instance, new DateTimeOffset(reader.GetDateTime(column.Key)), null);
            else if (column.Value.IsString)
                column.Value.Property.SetValue(instance, reader.GetString(column.Key), null);
            else
            {
                column.Value.Property.SetValue(instance, reader.GetValue(column.Key), null);
            }
        }

        protected abstract void AddCommandParameters(IIntwentySqlParameter[] parameters, IDbCommand command);
            

        protected abstract void InferAutoIncrementalValue<T>(IntwentyDbTableDefinition info, List<IntwentySqlParameter> parameters, T entity, IDbCommand command);

        protected string GetJSONValue(IDataReader reader, int index)
        {
            if (reader.IsDBNull(index))
                return string.Empty;

            var columnname = reader.GetName(index);
            var datatypename = reader.GetDataTypeName(index);

            if (IsNumeric(datatypename))
                return "\"" + columnname + "\":" + Convert.ToString(reader.GetValue(index)).Replace(",", ".");
            else if (IsDateTime(datatypename))
                return "\"" + columnname + "\":" + "\"" + System.Text.Json.JsonEncodedText.Encode(Convert.ToDateTime(reader.GetValue(index)).ToString("yyyy-MM-dd")).ToString() + "\"";
            else
                return "\"" + columnname + "\":" + "\"" + System.Text.Json.JsonEncodedText.Encode(Convert.ToString(reader.GetValue(index))).ToString() + "\"";

        }

       

        protected virtual bool IsNumeric(string datatypename)
        {

            if (datatypename.ToUpper().Contains("NUMERIC"))
                return true;
            if (datatypename.ToUpper() == "REAL")
                return true;
            if (datatypename.ToUpper() == "INTEGER")
                return true;
            if (datatypename.ToUpper() == "INT")
                return true;
            if (datatypename.ToUpper().Contains("DECIMAL"))
                return true;


            return false;
        }

        protected virtual bool IsDateTime(string datatypename)
        {

            if (datatypename.ToUpper() == "TIMESTAMP")
                return true;

            if (datatypename.ToUpper() == "DATETIME")
                return true;
           

            return false;
        }

        private string GetJSONNullValue(string name)
        {
            return "\"" + name + "\":null";
        }

        private JsonObjectResult GetJsonObjectResult(IDataReader openreader)
        {
            var result = new JsonObjectResult();
            var sb = new StringBuilder();
            var separator = ' ';
            sb.Append("{");

            for (int i = 0; i < openreader.FieldCount; i++)
            {
                result.Values.Add(new ResultSetValue() { Name = openreader.GetName(i), Value = openreader.GetValue(i) });

                if (openreader.IsDBNull(i) && Options.JsonNullValueHandling == JsonNullValueMode.Exclude)
                    continue;

                if (openreader.IsDBNull(i))
                {
                    sb.Append(separator + GetJSONNullValue(openreader.GetName(i)));
                }
                else
                {
                    sb.Append(separator + GetJSONValue(openreader,i));
                }
                separator = ',';
            }
            sb.Append("}");       
            result.SetData(sb.ToString());
            return result;

        }

        private dynamic GetObject(IDataReader openreader)
        {
            var result = new ExpandoObject() as IDictionary<string, object>;

            for (int i = 0; i < openreader.FieldCount; i++)
            {
                if (openreader.IsDBNull(i) && Options.JsonNullValueHandling == JsonNullValueMode.Exclude)
                    continue;

                if (openreader.IsDBNull(i))
                {
                    result.Add(openreader.GetName(i), null);
                }
                else
                {
                    result.Add(openreader.GetName(i), openreader.GetValue(i));
                }
            }

            return result;

        }






    }
}
