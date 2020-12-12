using Intwenty.DataClient;
using Intwenty.DataClient.Reflection;
using System;
using System.Collections.Generic;
using System.ComponentModel.Design;

namespace DataClientTests
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Running tests...");
            Tests();
            Console.ReadLine();

        }

        private static void Tests()
        {
            var client = new Connection(DBMS.MariaDB, @"Server=127.0.0.1;Database=IntwentyDb;uid=root;Password=xxxxx");
            client.Open();

            try
            {

                if (client.TableExists("DataClient_TestTable"))
                    client.RunCommand("DROP TABLE DataClient_TestTable");

                client.CreateTable<DataClientTest>();

                for (int i = 0; i < 100; i++)
                    client.InsertEntity(new DataClientTest() { Name = "Dog " + i, BirthDate = DateTime.Now, DtOffset = DateTime.Now, TestValue = 2.34F });

                var t = client.GetEntities<DataClientTest>();

                foreach (var q in t)
                {
                    var s = client.GetEntity<DataClientTest>(q.Id);
                    s.Name = "Test " + q.Id;
                    s.TestValue = 777.77F;
                    s.DtOffset = null;
                    s.BirthDate = null;
                    client.UpdateEntity(s);

                }

                var nulltest = client.GetEntity<DataClientTest>(678234);

                t = client.GetEntities<DataClientTest>();

                t = client.GetEntities<DataClientTest>("select Name from DataClient_TestTable", false);

                t = client.GetEntities<DataClientTest>();

                var jsonarr = client.GetJsonArray("select id as \"Id\", name as \"Name\", testvalue as \"TestValue\" from DataClient_TestTable");

                var objlist = client.GetObjects("select * from DataClient_TestTable");

                var resultset = client.GetResultSet("select * from DataClient_TestTable");

                var json = client.GetJsonObject("select Name, TestValue from DataClient_TestTable");

                var typedresult = client.GetEntity<DataClientTest>("select Name, TestValue from DataClient_TestTable", false);

                var typedresult2 = client.GetEntity<DataClientTest>(t[5].Id);


            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            finally
            {
                client.Close();
            }
        }

     
    }


    [DbTablePrimaryKey("Id")]
    [DbTableName("DataClient_TestTable")]
    public class DataClientTest
    {
        [AutoIncrement]
        public int Id { get; set; }

        public string Name { get; set; }

        public DateTime? BirthDate { get; set; }

        [Ignore]
        public string StringField { get; set; }

        [Ignore]
        public List<DataClientTest> ListField { get; set; }

        public DateTimeOffset? DtOffset { get; set; }

        public float? TestValue { get; set; }

       

    }

   

}
