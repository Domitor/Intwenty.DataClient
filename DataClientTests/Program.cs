using Intwenty.DataClient;
using Intwenty.DataClient.Reflection;
using System;

namespace DataClientTests
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Running tests...");

            Test1();
            Test2();

            Console.ReadLine();

        }

        private static void Test1()
        {
            try
            {
                var client = new Connection(DBMS.MariaDB, @"Server=127.0.0.1;Database=IntwentyDb;uid=root;Password=xxxxxxx");
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
                var client = new Connection(DBMS.MariaDB, @"Server=127.0.0.1;Database=IntwentyDb;uid=root;Password=thriller");


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
                var client = new Connection(DBMS.MariaDB, @"Server=127.0.0.1;Database=IntwentyDb;uid=root;Password=thriller");
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

     





    }

    [DbTablePrimaryKey("Id")]
    [DbTableName("DataClient_CarsTest")]
    public class Cars
    {
        [AutoIncrement]
        public int Id { get; set; }

        public string ModelName { get; set; }

    }

}
