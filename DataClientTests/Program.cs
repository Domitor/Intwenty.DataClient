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

            //Test1();
            //Test2();
            //Test3();
            Test4();
           
            Console.ReadLine();

        }

        private static void Test1()
        {
            try
            {
                var client = new Connection(DBMS.MariaDB, @"Server=127.0.0.1;Database=IntwentyDb;uid=root;Password=xxxx");
                client.Open();

                client.CreateTable<Cars>();

                for (int i = 0; i < 10; i++)
                    client.InsertEntity(new Cars() { ModelName = "Volvo " + i });

                client.Close();


            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }

        private static void Test2()
        {
            try
            {
                var client = new Connection(DBMS.MariaDB, @"Server=127.0.0.1;Database=IntwentyDb;uid=root;Password=xxxx");


                for (int i = 0; i < 10; i++)
                {
                    client.Open();
                    try
                    {
                        client.RunCommand(string.Format("CREATE UNIQUE INDEX {0}_Idx1 ON {0} (Id, ModelName)", "DataClient_CarsTest"));
                    }
                    catch { }
                    finally { client.Close(); }

                }





            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }

        private static void Test3()
        {
            try
            {
                var client = new Connection(DBMS.MariaDB, @"Server=127.0.0.1;Database=IntwentyDb;uid=root;Password=xxxx");
                client.Open();

                var t = client.GetEntities<Cars>();

                foreach (var q in t)
                {
                    var s = client.GetEntity<Cars>(q.Id);
                    client.DeleteEntity(s);
                }


                client.Close();


            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }

        private static void Test4()
        {
            try
            {
                var client = new Connection(DBMS.MariaDB, @"Server=127.0.0.1;Database=IntwentyDb;uid=root;Password=thriller", new DataClientOptions() { JsonNullValueHandling = JsonNullValueMode.Exclude } );

                /*
                client.Open();

                if (client.TableExists("DataClient_PetsTest"))
                {

                    client.RunCommand("DROP TABLE DataClient_PetsTest");
                }

                client.CreateTable<Pets>();

                for (int i = 0; i < 100000; i++)
                    client.InsertEntity(new Pets() { Name = "Dog " + i, BirthDate = DateTime.Now, Offset = DateTime.Now, TestValue = 2.34F });

                client.Close();
          

                client.Open();

                var t = client.GetEntities<Pets>();

                foreach (var q in t)
                {
                    var s = client.GetEntity<Pets>(q.Id);
                    s.Name = "Test " + q.Id;
                    s.TestValue = 777.77F;
                    s.Offset = null;
                    s.BirthDate = null;
                    client.UpdateEntity(s);
                   
                }
                      */

                client.Open();

               //var t = client.GetEntities<Pets>();

                var t = client.GetEntities<Pets>("select Name from DataClient_PetsTest", false);

                t = client.GetEntities<Pets>();

                var jsonarr = client.GetJsonArray("select * from DataClient_PetsTest");

                var objlist = client.GetObjects("select * from DataClient_PetsTest");

                var resultset = client.GetResultSet("select * from DataClient_PetsTest");

                var json = client.GetJsonObject("select Name, TestValue from DataClient_PetsTest");

                var typedresult = client.GetEntity<Pets>("select Name, TestValue from DataClient_PetsTest", false);

                var typedresult2 = client.GetEntity<Pets>(t[5].Id);

                client.Close();


            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }

    }

    [DbTablePrimaryKey("Id")]
    [DbTableName("DataClient_CarsTest")]
    public class Cars
    {
        [AutoIncrement]
        public int Id { get; set; }

        public string ModelName { get; set; }

    }

    [DbTablePrimaryKey("Id")]
    [DbTableName("DataClient_PetsTest")]
    public class Pets
    {
        [AutoIncrement]
        public int Id { get; set; }

        public string Name { get; set; }

        public DateTime? BirthDate { get; set; }

        [Ignore]
        public string MoronField { get; set; }

        [Ignore]
        public List<Pets> MoronField2 { get; set; }

        public DateTimeOffset? Offset { get; set; }

        public float? TestValue { get; set; }

       

    }

   

}
