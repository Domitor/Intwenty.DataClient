![alt text](https://github.com/Domitor/Intwenty/blob/master/IntwentyDemo/wwwroot/images/intwenty_loggo_small.png)

# Intwenty.DataClient
A .net core database client library that includes ORM functions, JSON support and more.

## Description
Intwenty.DataClient is a laser fast database client library with a limited set of functions for object relational mapping and generation JSON directly from SQL query results. 

## Implementation
Instead of extending the IDbConnection Intwenty.DataClient wraps around other libraries that implements IDbConnection and IDbCommand. It can be seen as a generic abstraction layer that allows users to switch database without much concern of sql flavour. For all methods that return data the DataReader of the underlying library is used internally.

### Included libraries
* MySqlConnector
* NpgSql
* System.Data.SqlClient
* System.Data.SQLite.Core

## Supported Databases
Intwenty.DataClient is built as a wrapper around popular client libraries for MS SQLServer, MariaDb, Sqlite and Postgres. This means that all ORM functions and other functions that generates sql is guranteed to work in all databases.



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
        List<T> GetEntities<T>() where T : new();
        List<T> GetEntities<T>(string sql, bool isprocedure=false, IIntwentySqlParameter[] parameters=null) where T : new();
        string GetJSONObject(string sql, bool isprocedure = false, IIntwentySqlParameter[] parameters = null, IIntwentyResultColumn[] resultcolumns = null);
        string GetJSONArray(string sql, int minrow = 0, int maxrow = 0, bool isprocedure = false, IIntwentySqlParameter[] parameters = null, IIntwentyResultColumn[] resultcolumns = null);
        IResultSet GetResultSet(string sql, int minrow = 0, int maxrow = 0, bool isprocedure = false, IIntwentySqlParameter[] parameters = null, IIntwentyResultColumn[] resultcolumns = null);
        DataTable GetDataTable(string sql, int minrow = 0, int maxrow = 0, bool isprocedure = false, IIntwentySqlParameter[] parameters = null, IIntwentyResultColumn[] resultcolumns = null);
        int InsertEntity<T>(T entity);
        int UpdateEntity<T>(T entity);
        int DeleteEntity<T>(T entity);
        int DeleteEntities<T>(IEnumerable<T> entities);
        List<TypeMapItem> GetDbTypeMap();
        List<CommandMapItem> GetDbCommandMap();
        
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


