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

            //var totalCount = 10000000;
            //var eachCount = 500000;
            //var entities = Enumerable.Range(0, totalCount).Select(i => new DemoTable()
            //{
            //    Value = "testValue" + i
            //});


            //var groupList = new List<IEnumerable<DemoTable>>();
            //var index = 0;
            //var listItem = new List<DemoTable>();
            //foreach (var item in entities)
            //{
            //    listItem.Add(item);

            //    if ((++index) % eachCount == 0)
            //    {
            //        groupList.Add(listItem);
            //        listItem = new List<DemoTable>();
            //    }
            //}

            //foreach (var eachItem in groupList)
            //{
            //    Console.WriteLine($"插入{eachItem.Count()}条");
            //    ShardingTableManager<DemoTable>.InsertToSeparateTable("@Value,@CreateTime", eachItem, "DemoTable", ShardingType.Count);
            //}
            #endregion

            #region 查询

            //var result = ShardingTableManager<DemoTable>.QueryAll("DemoTable", "", new { Id = "10000" },"count(1)");
            //var result = ShardingTableManager<DemoTable>.QueryAll("DemoTable", "where Value=@value", new { value = "testValue400267" });
            //foreach (var item in result)
            //{
            //    Console.Write(item.Value + ",");
            //}

            //分页
            var r1 = ShardingTableManager<DemoTable>.GetPageSql(300000, 2, "*", "DemoTable0", "", "", "desc", "Id");

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
