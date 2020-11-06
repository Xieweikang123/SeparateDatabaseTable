using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DbHelper;
using SeparateDatabaseTable.Models;
using SeparateDataHelper;

namespace SeparateDatabaseTable
{
    class Program
    {
        static void Main(string[] args)
        {

            #region 创建数据
            //var entities = Enumerable.Range(0, 10000).Select(i => new DemoTable()
            //{
            //    Value = "testValue" + i
            //});


            //var groupList = new List<IEnumerable<DemoTable>>();
            //var index = 0;
            //var listItem = new List<DemoTable>();
            //foreach (var item in entities)
            //{
            //    listItem.Add(item);

            //    if ((++index) % 2000 == 0)
            //    {
            //        groupList.Add(listItem);
            //        listItem = new List<DemoTable>();
            //    }
            //}

            //foreach (var eachItem in groupList)
            //{
            //    SeparateTableManager.InsertToSeparateTable("@Value,@CreateTime", eachItem, "DemoTable", ShardingType.Count);
            //}
            #endregion

            #region 查询

            var result= ShardingTableManager.Query<DemoTable>("DemoTable", "", null);
            #endregion

            //SeparateTableManager.InsertToSeparateTable("@Value,@CreateTime", entities, "DemoTable", ShardingType.Count);


            //查询
            //var tableName= SeparateTableManager.GetTableSuffixNameByDateTime("DemoTable", DateTime.Now, ShardingType.Day);
            //var list= DapperHelper.QueryList<DemoTable>($"select * from {tableName}", null);


            //var list1= DapperHelper.QueryList<DemoTable>($"select * from {tableName} where CreateTime between @dt1 and @dt2", new{dt1=DateTime.Now.AddHours(-10),dt2= "2020-11-06 11:47:21.580" });

            //DapperHelper.Insert("insert into DemoTable Values(@Value,@CreateTime)", entities);

            //foreach (var item in entities) {
            //     SeparateTableManager.InsertToSeparateTable("@Value,@CreateTime", item, "DemoTable_");
            //}



            Console.WriteLine("ok");

            Console.ReadKey();
        }
    }
}
