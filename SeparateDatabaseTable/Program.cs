using System;
using System.CodeDom;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DbHelper;
using Models;
using SeparateDataHelper;

namespace SeparateDatabaseTable
{
    class Program
    {
        static void Main(string[] args)
        {
            CreateData();


            #region 查询

            //var stopWatch=new System.Diagnostics.Stopwatch();
            //stopWatch.Start();

            //var result = ShardingTableManager<DemoTable>.QueryAll("DemoTable", "where Value=@Value", new { Value = "testValue233" });

            //var result = ShardingTableManager<DemoTable>.GetPageEntities(20000 ,2, "*", "DemoTable", "", "AddTime", "asc", "Id");
            //var result1 = ShardingTableManager<DemoTable>.GetPageEntities(21000, 2, "*", "DemoTable", "", "AddTime", "asc", "Id");


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

        private static void CreateData()
        {
            #region 创建数据

            var totalCount = 1000;
            ShardingTableManager<DemoTable>.eachTableSize = 300;

            //var time = DateTime.Now.AddMinutes(new Random().Next(1000));
            var dateNow = DateTime.Now;
            var random = new Random();
            //var testData = dateNow.AddMinutes(random.Next(2000));
            //var entities = Enumerable.Range(0, totalCount).Select(i => new DemoTable()
            //{
            //    Id = Guid.NewGuid(),
            //    Value = "testValue" + i,
            //    AddTime = dateNow.AddSeconds(random.Next(2000))
            //});
            var entities = new List<DemoTable>();

            var dataTable = new DataTable();
            dataTable.Columns.AddRange(new DataColumn[] {
                new DataColumn("Id",typeof(Guid)),
                new DataColumn("Value",typeof(string)),
                new DataColumn("AddTime",typeof(DateTime))
            });
            for (var i = 0; i < totalCount; i++)
            {
                var demoTable = new DemoTable()
                {
                    Id = Guid.NewGuid(),
                    Value = "testValue" + i,
                    AddTime = dateNow.AddSeconds(random.Next(2000))
                };
                entities.Add(demoTable);
                var dr = dataTable.NewRow();
                dr[0] = demoTable.Id;
                dr[1] = demoTable.Value;
                dr[2] = demoTable.AddTime;
                dataTable.Rows.Add(dr);
            }

            ShardingTableManager<DemoTable>.InsertSqlBulk(dataTable, "DemoTable");
            //DapperHelper.Insert<DemoTable>("insert into DemoTable values(@Id,@Value,@AddTime)", entities);
            ShardingTableManager<DemoTable>.tableNamePrefix = "DemoTable";
            var groupList = new List<DataTable>();
            var addDt = dataTable.Clone();
            var index = 0;
            foreach (DataRow row in dataTable.Rows)
            {
                var newRow = addDt.NewRow();
                newRow[0] = row[0];
                newRow[1] = row[1];
                newRow[2] = row[2];
                addDt.Rows.Add(newRow);
                if ((++index) % ShardingTableManager<DemoTable>.eachTableSize == 0)
                {
                    groupList.Add(addDt);
                    addDt = new DataTable();
                    addDt = dataTable.Clone();
                }
            }

            if (addDt.Rows.Count > 0) {
                groupList.Add(addDt);
            }

            var tablePrefix = 0;
            foreach (var isnertDt in groupList)
            {
                Console.WriteLine($"插入{isnertDt.Rows.Count}条");
                ShardingTableManager<DemoTable>.InsertSqlBulk(isnertDt, "DemoTable" + tablePrefix++);
            }


            //var groupList = new List<IEnumerable<DemoTable>>();
            //var index = 0;
            //var listItem = new List<DemoTable>();
            //foreach (var item in entities)
            //{
            //    listItem.Add(item);

            //    if ((++index) % ShardingTableManager<DemoTable>.eachTableSize == 0)
            //    {
            //        groupList.Add(listItem);
            //        listItem = new List<DemoTable>();
            //    }
            //}

            //foreach (var eachItem in groupList)
            //{
            //    Console.WriteLine($"插入{eachItem.Count()}条");
            //    ShardingTableManager<DemoTable>.InsertToSeparateTable("@Id,@Value,@AddTime", eachItem, "DemoTable", ShardingType.Count);
            //}
            #endregion
        }
    }
}
