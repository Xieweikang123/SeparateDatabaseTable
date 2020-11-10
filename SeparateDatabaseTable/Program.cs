using System;
using System.CodeDom;
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



            #region 查询

            //var stopWatch=new System.Diagnostics.Stopwatch();
            //stopWatch.Start();

            //var result = ShardingTableManager<DemoTable>.QueryAll("DemoTable", "where Value=@Value", new { Value = "testValue233" });

            //var result = ShardingTableManager<DemoTable>.GetPageEntities(20000 ,2, "*", "DemoTable", "", "AddTime", "asc", "Id");
            var result1 = ShardingTableManager<DemoTable>.GetPageEntities(21000, 2, "*", "DemoTable", "", "AddTime", "asc", "Id");
            

            //stopWatch.Stop();
            //Console.WriteLine("task1 :"+stopWatch.Elapsed);
            //var result = ShardingTableManager<DemoTable>.QueryAll("DemoTable", "where Value=@value", new { value = "testValue400267" });
            //foreach (var item in result)
            //{
            //    Console.Write(item.Value + ",");
            //}

            //分页
            //var r1 = ShardingTableManager<DemoTable>.GetPageSql(300000, 2, "*", "DemoTable", "", "", "desc", "Id");

            #endregion

            //SeparateTableManager.InsertToSeparateTable("@Value,@CreateTime", entities, "DemoTable", ShardingType.Count);



            //stopWatch.Restart();
            //var list1 = DapperHelper.QueryList<DemoTable>($" SELECT * FROM dbo.DemoTable0 UNION ALL SELECT * FROM dbo.DemoTable1 UNION ALL SELECT * FROM dbo.DemoTable2 UNION ALL SELECT * FROM dbo.DemoTable3 UNION ALL SELECT * FROM dbo.DemoTable4", null);
            //stopWatch.Stop();
            //Console.WriteLine("task2 :"+ stopWatch.Elapsed);

            //DapperHelper.Insert("insert into DemoTable Values(@Value,@CreateTime)", entities);

            //foreach (var item in entities) {
            //     SeparateTableManager.InsertToSeparateTable("@Value,@CreateTime", item, "DemoTable_");
            //}



            Console.WriteLine("ok");

            Console.ReadKey();
        }

        private static void CreateData() {
            #region 创建数据

            var totalCount = 100000;
            var eachCount = 20000;
       
            var entities = Enumerable.Range(0, totalCount).Select(i => new DemoTable()
            {
                Id = Guid.NewGuid(),
                Value = "testValue" + i,
            });


            var groupList = new List<IEnumerable<DemoTable>>();
            var index = 0;
            var listItem = new List<DemoTable>();
            foreach (var item in entities)
            {
                listItem.Add(item);

                if ((++index) % eachCount == 0)
                {
                    groupList.Add(listItem);
                    listItem = new List<DemoTable>();
                }
            }

            foreach (var eachItem in groupList)
            {
                Console.WriteLine($"插入{eachItem.Count()}条");
                ShardingTableManager<DemoTable>.InsertToSeparateTable("@Id,@Value,@AddTime", eachItem, "DemoTable", ShardingType.Count);
            }
            #endregion
        }
    }
}
