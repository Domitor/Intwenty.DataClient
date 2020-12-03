![alt text](https://github.com/Domitor/Intwenty/blob/master/IntwentyDemo/wwwroot/images/intwenty_loggo_small.png)

# Intwenty.DataClient
A .net core database client library that includes ORM functions, JSON support and more.

## Description
Intwenty.DataClient is a laser fast database client library with a limited set of functions for object relational mapping and generating JSON directly from SQL query results. 

## Implementation
Instead of extending the IDbConnection Intwenty.DataClient works as a generic abstraction layer and wraps around other libraries that implements IDbConnection and IDbCommand. All methods on the IDataClient interface that not explicitly takes an sql statment as parameter (GetEntity<T>, InsertEntity<T> etc) generates sql for all supported databases.

## Performance
This is a very fast library but it relies on the DbCommand and the DataReader of the underlying libraries.

## Included libraries
* MySqlConnector
* NpgSql
* System.Data.SqlClient
* System.Data.SQLite.Core

## Supported Databases
Intwenty.DataClient is built as a wrapper around popular client libraries for MS SQLServer, MariaDb, Sqlite and Postgres. This means that all ORM functions and other functions that generates sql is guranteed to work in all databases.

## Json String Functions
Except for methods for retrieving data as objects of type T, and dynamic, there is also the functions: GetJsonObject and GetJsonArray which returns objects including json strings constructed directly from the datareader of the underlying library.

## Example

    [DbTablePrimaryKey("Id")]
    public class Person {
        [AutoIncrement]
        public int Id { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
    }
    
    var client = new Connection(DBMS.MariaDb, "MyConnectionString");
    client.Open();
    client.CreateTable<Person>();
    
    //Insert some rows
    for (int i = 0; i < 5000; i++)
         client.InsertEntity(new Person() { FirstName = "Donald", LastName = "Duck"  });
         
     //Get a list of person objects
     var persons = client.GetEntities<Person>();
     
     //Get a filtered list of person objects
     var persons = client.GetEntities<Person>("select * from person where id>@P1", new IIntwentySqlParameter[] {new IntwentySqlParameter("@P1", 1000) });
     
      //Get persons as a json string
     var persons = client.GetJSONArry("select * from person");
  
    client.Close();
    
    

## The IDataClient interface

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
        
## Annotations
Intwenty.DataClient uses it own set of annotations to support ORM functions

       [DbTableIndex("IDX_1", false, "Col2")]
       [DbTablePrimaryKey("Col1")]
       [DbTableName("MyDbTable")]
       public class Example 
       { 
          public int Col1 { get; set; }
          public int Col2 { get; set; }
        
          [DbColumnName("MyDbColumn")]
          public string Col3 { get; set; }
        
          [NotNull]
          public int Col4 { get; set; }
        
          [Ignore]
          public int Col5 { get; set; }
       }


