# Intwenty.DataClient
A .net core database client library that includes ORM functions, JSON support and more.

## Description
Intwenty.DataClient is a lightning fast database client library with a limited set of functions for object relational mapping and generation JSON directly from SQL query results. 

## Supported Databases
Intwenty.DataClient is built as a wrapper around popular client libraries for MS SQLServer, MariaDb, Sqlite and Postgres. 

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


