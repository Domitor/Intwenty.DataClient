using Intwenty.DataClient.Model;
using Intwenty.DataClient.Reflection;
using System;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Text.Json;

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
        public abstract void Open();
        public abstract void Close();
        protected abstract BaseSqlBuilder GetSqlBuilder();



        public List<TypeMapItem> GetDbTypeMap()
        {
            return TypeMap.GetTypeMap();
        }

        public List<CommandMapItem> GetDbCommandMap()
        {
            return CommandMap.GetCommandMap();
        }

        public void BeginTransaction()
        {
            IsInTransaction = true;
        }

        public void CommitTransaction()
        {
            var transaction = GetTransaction();
            if (IsInTransaction && transaction != null)
            {
                IsInTransaction = false;
                transaction.Commit();
            }
           
        }

        public void RollbackTransaction() 
        {
            var transaction = GetTransaction();
            if (IsInTransaction && transaction != null)
            {
                IsInTransaction = false;
                transaction.Rollback();
            }
        }


        public void RunCommand(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
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

        public object GetScalarValue(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
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

        public dynamic GetObject(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            object result = null;


            using (var command = GetCommand())
            {
                command.CommandText = sql;
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

        public IJsonObjectResult GetJsonObject(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            JsonObjectResult result = null;

            using (var command = GetCommand())
            {
                command.CommandText = sql;
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

        public List<dynamic> GetObjects(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            var res = new List<dynamic>();

            using (var command = GetCommand())
            {
                command.CommandText = sql;
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

        public IJsonArrayResult GetJsonArray(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            var start = DateTime.Now;
            var result = new JsonArrayResult();

            using (var command = GetCommand())
            {
                command.CommandText = sql;
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

        public IResultSet GetResultSet(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            var res = new ResultSet();

            using (var command = GetCommand())
            {
                command.CommandText = sql;
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

        public DataTable GetDataTable(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null)
        {
            var table = new DataTable();

            using (var command = GetCommand())
            {
                command.CommandText = sql;
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

        public void CreateTable<T>()
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


        public bool TableExists<T>()
        {
            var info = TypeDataHandler.GetDbTableDefinition<T>();
            return TableExists(info.Name);
        }

        public bool TableExists(string tablename)
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

        public bool ColumnExists(string tablename, string columnname)
        {
            try
            {
                var checkcommand = GetCommand();
                checkcommand.CommandText = string.Format("SELECT {0} FROM {1} WHERE 1=2", columnname, tablename);
                checkcommand.CommandType = CommandType.Text;
                checkcommand.ExecuteScalar();

                return true;
            }
            catch { }
          
            return false;
        }

        public virtual T GetEntity<T>(int id) where T : new()
        {
            return GetEntityInternal<T>(id);
        }

        public virtual T GetEntity<T>(string id) where T : new()
        {
            return GetEntityInternal<T>(id);
        }

        private T GetEntityInternal<T>(object id) where T : new()
        {
            var res = new T();
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

        public virtual T GetEntity<T>(string sql, bool isprocedure) where T : new()
        {
            return GetEntity<T>(sql, isprocedure, null);
        }

        public virtual T GetEntity<T>(string sql, bool isprocedure, IIntwentySqlParameter[] parameters = null) where T : new()
        {

            var res = new T();
            var info = TypeDataHandler.GetDbTableDefinition<T>();

            using (var command = GetCommand())
            { 
                command.CommandText = sql;
                if (isprocedure)
                    command.CommandType = CommandType.StoredProcedure;
                else
                    command.CommandType = CommandType.Text;

                AddCommandParameters(parameters, command);

                var reader = command.ExecuteReader();

                while (reader.Read())
                {
                   
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
        public virtual List<T> GetEntities<T>(string sql, bool isprocedure = false) where T : new() 
        {
            return GetEntities<T>(sql, isprocedure, null);
        }
        public virtual List<T> GetEntities<T>(string sql, bool isprocedure, IIntwentySqlParameter[] parameters = null) where T : new()
        {

            var res = new List<T>();
            var info = TypeDataHandler.GetDbTableDefinition<T>();
            var readercolumns = new Dictionary<int, IntwentyDbColumnDefinition>();

            using (var command = GetCommand())
            {
                command.CommandText = sql;
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


        public virtual int InsertEntity<T>(T entity)
        {
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

        public int InsertEntity(string json, string tablename)
        {
            throw new NotImplementedException();
        }

        public int InsertEntity(JsonElement json, string tablename)
        {
            throw new NotImplementedException();
        }


        public int UpdateEntity<T>(T entity)
        {
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

        public int UpdateEntity(string json, string tablename)
        {
            throw new NotImplementedException();
        }

        public int UpdateEntity(JsonElement json, string tablename)
        {
            throw new NotImplementedException();
        }

        public int DeleteEntities<T>(IEnumerable<T> entities)
        {
            var res = 0;
            foreach (var t in entities)
            {
                res += DeleteEntity(t);
            }
            return res;
        }

        public int DeleteEntity<T>(T entity)
        {
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

        protected abstract IDbCommand GetCommand();

        protected abstract IDbTransaction GetTransaction();

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

        protected string GetJSONNullValue(string name)
        {
            return "\"" + name + "\":null";
        }

        protected bool IsNumeric(string datatypename)
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

        protected bool IsDateTime(string datatypename)
        {

            if (datatypename.ToUpper() == "TIMESTAMP")
                return true;

            if (datatypename.ToUpper() == "DATETIME")
                return true;
           

            return false;
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
