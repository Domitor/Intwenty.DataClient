using Intwenty.DataClient;
using Intwenty.DataClient.Reflection;
using System;
using System.Collections.Generic;

namespace DataClientTests
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Running tests...");

            Test1();

            Console.ReadLine();

        }

        private static void Test1()
        {
            try
            {
                var client = new Connection(DBMS.SQLite, @"");
                client.Open();
                
                client.Close();


            }
            catch (Exception ex)
            { 

            }
        }

      


    }
}
