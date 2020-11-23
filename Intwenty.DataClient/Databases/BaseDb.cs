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

        public dynamic GetObject(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null, IIntwentyResultColumn[] resultcolumns = null)
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
                    var columns = GetQueryColumns(reader, resultcolumns);
                    result = GetObject(reader, columns);
                    break;
                }

                reader.Close();
                reader.Dispose();
            }

            return result;
        }

        public IJsonObjectResult GetJsonObject(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null, IIntwentyResultColumn[] resultcolumns = null)
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
                    var columns = GetQueryColumns(reader, resultcolumns);
                    result = GetJsonObjectResult(reader, columns);
                    break;
                }

                reader.Close();
                reader.Dispose();

            }

            if (result == null)
                result = new JsonObjectResult();


            return result;
        }

        public List<dynamic> GetObjects(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null, IIntwentyResultColumn[] resultcolumns = null)
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

                var columns = new List<IntwentyResultColumn>();
                while (reader.Read())
                {

                    if (columns.Count == 0)
                        columns = GetQueryColumns(reader, resultcolumns);

                    res.Add(GetObject(reader, columns));

                }

                reader.Close();
                reader.Dispose();
            }

            return res;
        }

        public IJsonArrayResult GetJsonArray(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null, IIntwentyResultColumn[] resultcolumns = null)
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


                var columns = new List<IntwentyResultColumn>();
                while (reader.Read())
                {

                    if (columns.Count == 0)
                        columns = GetQueryColumns(reader, resultcolumns);

                    var jsonobject = GetJsonObjectResult(reader, columns);
                    result.JsonObjects.Add(jsonobject);

                }

                reader.Close();
                reader.Dispose();

            }
            result.Duration = DateTime.Now.Subtract(start).TotalMilliseconds;
            result.ObjectCount = result.JsonObjects.Count;


            return result;
        }

        public IResultSet GetResultSet(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null, IIntwentyResultColumn[] resultcolumns = null)
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


                var columns = new List<IntwentyResultColumn>();
                var rindex = 0;

                while (reader.Read())
                {

                    rindex += 1;

                    if (columns.Count == 0)
                        columns = GetQueryColumns(reader, resultcolumns);

                    var row = new ResultSetRow();
                    foreach (var rc in columns)
                    {
                        if (reader.IsDBNull(rc.Index) && Options.JsonNullValueHandling == JsonNullValueMode.Exclude)
                            continue;

                        if (reader.IsDBNull(rc.Index))
                            row.SetValue(rc.Name, null);
                        else
                            row.SetValue(rc.Name, reader.GetValue(rc.Index));

                    }
                    res.Rows.Add(row);

                }

                reader.Close();
                reader.Dispose();
            }


            return res;
        }

        public DataTable GetDataTable(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null, IIntwentyResultColumn[] resultcolumns = null)
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

        public virtual T GetEntity<T>(string sql, bool isprocedure) where T : new()
        {
            return GetEntity<T>(sql, isprocedure, null);
        }

        public virtual T GetEntity<T>(string sql, bool isprocedure, IIntwentySqlParameter[] parameters = null) where T : new()
        {
            var key = sql.ToUpper().Replace(" ", "");
            if (key.Length > 100)
                key = key.Substring(0, 100);

            var res = default(T);
            var info = TypeDataHandler.GetDbTableDefinition<T>(key);

            using (var command = GetCommand())
            {
                command.CommandText = sql;
                if (isprocedure)
                    command.CommandType = CommandType.StoredProcedure;
                else
                    command.CommandType = CommandType.Text;

                AddCommandParameters(parameters, command);

                var reader = command.ExecuteReader();

                TypeDataHandler.AdjustToQueryResult(info, reader);

                while (reader.Read())
                {
                    var m = new T();
                    foreach (var col in info.Columns.OrderBy(p => p.Index))
                    {

                        if (reader.IsDBNull(col.Index))
                            continue;

                        SetPropertyValues(reader, col, m);

                    }
                    res = m;
                    break;
                }

                reader.Close();
                reader.Dispose();
            }

            return res;
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

                TypeDataHandler.AdjustToQueryResult(info, reader);

                while (reader.Read())
                {
                    foreach (var col in info.Columns.OrderBy(p => p.Index))
                    {

                        if (reader.IsDBNull(col.Index))
                            continue;

                        SetPropertyValues(reader, col, res);

                    }

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

            using (var command = GetCommand())
            {
                command.CommandText = string.Format("SELECT * FROM {0}", info.Name);
                command.CommandType = CommandType.Text;

                var reader = command.ExecuteReader();

                TypeDataHandler.AdjustToQueryResult(info, reader);

                while (reader.Read())
                {
                    var m = new T();
                    foreach (var col in info.Columns.OrderBy(p => p.Index))
                    {

                        if (reader.IsDBNull(col.Index))
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
        public virtual List<T> GetEntities<T>(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null) where T : new()
        {
            var key = sql.ToUpper().Replace(" ", "");
            if (key.Length > 100)
                key = key.Substring(0, 100);

            var res = new List<T>();
            var info = TypeDataHandler.GetDbTableDefinition<T>(key);

            using (var command = GetCommand())
            {
                command.CommandText = sql;
                if (isprocedure)
                    command.CommandType = CommandType.StoredProcedure;
                else
                    command.CommandType = CommandType.Text;

                AddCommandParameters(parameters, command);

                var reader = command.ExecuteReader();

                TypeDataHandler.AdjustToQueryResult(info, reader);

                while (reader.Read())
                {
                    var m = new T();
                    foreach (var col in info.Columns.OrderBy(p => p.Index))
                    {

                        if (reader.IsDBNull(col.Index))
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

        protected virtual void SetPropertyValues<T>(IDataReader reader, IntwentyDbColumnDefinition column, T instance)
        {

            if (column.IsInt32)
                column.Property.SetValue(instance, reader.GetInt32(column.Index), null);
            else if (column.IsBoolean)
                column.Property.SetValue(instance, reader.GetBoolean(column.Index), null);
            else if (column.IsDecimal)
                column.Property.SetValue(instance, reader.GetDecimal(column.Index), null);
            else if (column.IsSingle)
                column.Property.SetValue(instance, reader.GetFloat(column.Index), null);
            else if (column.IsDouble)
                column.Property.SetValue(instance, reader.GetDouble(column.Index), null);
            else if (column.IsDateTime)
                column.Property.SetValue(instance, reader.GetDateTime(column.Index), null);
            else if (column.IsDateTimeOffset)
                column.Property.SetValue(instance, new DateTimeOffset(reader.GetDateTime(column.Index)), null);
            else if (column.IsString)
                column.Property.SetValue(instance, reader.GetString(column.Index), null);
            else
            {
                column.Property.SetValue(instance, reader.GetValue(column.Index), null);
            }
        }

        protected abstract void AddCommandParameters(IIntwentySqlParameter[] parameters, IDbCommand command);
            

        protected abstract void InferAutoIncrementalValue<T>(IntwentyDbTableDefinition info, List<IntwentySqlParameter> parameters, T entity, IDbCommand command);

        protected string GetJSONValue(IDataReader r, IntwentyResultColumn resultcol)
        {
            if (r.IsDBNull(resultcol.Index))
                return string.Empty;

            var columnname = resultcol.Name;
            var datatypename = r.GetDataTypeName(resultcol.Index);

            if (IsNumeric(datatypename, resultcol))
                return "\"" + columnname + "\":" + Convert.ToString(r.GetValue(resultcol.Index)).Replace(",", ".");
            else if (IsDateTime(datatypename, resultcol))
                return "\"" + columnname + "\":" + "\"" + System.Text.Json.JsonEncodedText.Encode(Convert.ToDateTime(r.GetValue(resultcol.Index)).ToString("yyyy-MM-dd")).ToString() + "\"";
            else
                return "\"" + columnname + "\":" + "\"" + System.Text.Json.JsonEncodedText.Encode(Convert.ToString(r.GetValue(resultcol.Index))).ToString() + "\"";

        }

        protected string GetJSONNullValue(IntwentyResultColumn resultcol)
        {
            return "\"" + resultcol.Name + "\":null";

        }

        protected bool IsNumeric(string datatypename, IntwentyResultColumn resultcolumn)
        {
            if (resultcolumn.IsNumeric)
                return true;

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

        protected bool IsDateTime(string datatypename, IntwentyResultColumn resultcolumn)
        {
            if (resultcolumn.IsDateTime)
                return true;

            if (datatypename.ToUpper() == "TIMESTAMP")
                return true;

            if (datatypename.ToUpper() == "DATETIME")
                return true;
           

            return false;
        }

        private List<IntwentyResultColumn> GetQueryColumns(IDataReader reader, IIntwentyResultColumn[] resultcolumns)
        {
            var res = new List<IntwentyResultColumn>();


            if (resultcolumns == null || (resultcolumns != null && resultcolumns.Count() == 0))
            {
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var rc = new IntwentyResultColumn() { Name = reader.GetName(i), Index = i };
                    res.Add(rc);
                }
            }
            else
            {
                for (int c = 0; c < resultcolumns.Length; c++)
                {
                    var col = resultcolumns[c];
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        if (reader.GetName(i).ToLower() == col.Name.ToLower())
                        {
                            res.Add(new IntwentyResultColumn() { Name = col.Name, Index = i });
                            break;
                        }
                    }

                 }
            }

         
            return res;
        }


        private JsonObjectResult GetJsonObjectResult(IDataReader openreader, List<IntwentyResultColumn> columns)
        {
            var result = new JsonObjectResult();
            var sb = new StringBuilder();
            var separator = ' ';
            sb.Append("{");
            foreach (var rc in columns)
            {
                result.Values.Add(new ResultSetValue() { Name = rc.Name, Value = openreader.GetValue(rc.Index) });

                if (openreader.IsDBNull(rc.Index) && Options.JsonNullValueHandling == JsonNullValueMode.Exclude)
                    continue;

                if (openreader.IsDBNull(rc.Index))
                {
                    sb.Append(separator + GetJSONNullValue(rc));
                }
                else
                {
                    sb.Append(separator + GetJSONValue(openreader, rc));
                }
                separator = ',';
            }
            sb.Append("}");       
            result.SetData(sb.ToString());
            return result;

        }

        private dynamic GetObject(IDataReader openreader, List<IntwentyResultColumn> columns)
        {
            var result = new ExpandoObject() as IDictionary<string, object>;

            foreach (var rc in columns)
            {

                if (openreader.IsDBNull(rc.Index) && Options.JsonNullValueHandling == JsonNullValueMode.Exclude)
                    continue;

                if (openreader.IsDBNull(rc.Index))
                {
                    result.Add(rc.Name, openreader.GetValue(rc.Index));
                }
                else
                {
                    result.Add(rc.Name, null);
                }
            }

            return result;

        }






    }
}
