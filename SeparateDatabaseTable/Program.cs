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

            var tablename = SeparateTableManager.GetLastInsertTableName("DemoTable_");

            var demoTableEntity=new DemoTable(){Value="2333"};
           
            var entities= Enumerable.Range(0, 10).Select(i => new DemoTable()
            {
                Value = "name" + i
            });
            SeparateTableManager.InsertToSeparateTable("@Value,@CreateTime", entities, "DemoTable_", 3);


            //var r1 = SqlHelper.ExecuteScalar($"select count(1) from {tablename}");

            //var usersList = Enumerable.Range(0, 10).Select(i => new DemoTable()
            //{
            //    Value = "name" + i
            //});
            //DapperHelper.Insert("", usersList);

        }
    }
}
