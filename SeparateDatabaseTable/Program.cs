using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using  DbHelper;
using SeparateDatabaseTable.Models;
using SeparateDataHelper;

namespace SeparateDatabaseTable
{
    class Program
    {
        static void Main(string[] args) {
            //SeparateTableManager.tableNamePrefix = "DemoTable_";


            var demoTableEntity=new DemoTable(){Value="2333"};
           
            var entities= Enumerable.Range(0, 1000).Select(i => new DemoTable()
            {
                Value ="testValue" + i
            });

            SeparateTableManager.InsertToSeparateTable("@Value,@CreateTime", entities, "DemoTable", ShardingType.Day);



            //DapperHelper.Insert("insert into DemoTable Values(@Value,@CreateTime)", entities);

            //foreach (var item in entities) {
            //     SeparateTableManager.InsertToSeparateTable("@Value,@CreateTime", item, "DemoTable_");
            //}


            //var r1 = SqlHelper.ExecuteScalar($"select count(1) from {tablename}");

            //var usersList = Enumerable.Range(0, 10).Select(i => new DemoTable()
            //{
            //    Value = "name" + i
            //});
            //DapperHelper.Insert("", usersList);

            Console.WriteLine("ok");

            Console.ReadKey();
        }
    }
}
