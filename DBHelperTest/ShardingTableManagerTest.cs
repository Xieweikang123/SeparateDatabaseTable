using System;
using System.Collections.Generic;
using System.Linq;
using DbHelper;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Models;

namespace DBHelperTest
{
    [TestClass]
    public class ShardingTableManagerTest
    {
        [TestMethod]
        public void TestSecondarySearchPaging() {
            var result1 = ShardingTableManager<DemoTable>.SecondarySearchPaging(3, 2, "DemoTable", "AddTime", "asc");
            var ep1 = DapperHelper.QueryList<DemoTable>(
                $"SELECT * FROM dbo.DemoTable ORDER BY AddTime  OFFSET 3 ROWS FETCH NEXT 3 ROWS ONLY", null);
            IsEqual(ep1,result1);

            var result2 = ShardingTableManager<DemoTable>.SecondarySearchPaging(3, 1, "DemoTable", "AddTime", "asc");
            var ep2 = DapperHelper.QueryList<DemoTable>(
                $"SELECT * FROM dbo.DemoTable ORDER BY AddTime  OFFSET 0 ROWS FETCH NEXT 3 ROWS ONLY", null);
            IsEqual(ep2, result2);

            var result3 = ShardingTableManager<DemoTable>.SecondarySearchPaging(15, 2, "DemoTable", "AddTime", "asc");
            var ep3 = DapperHelper.QueryList<DemoTable>(
                $"SELECT * FROM dbo.DemoTable ORDER BY AddTime  OFFSET 15 ROWS FETCH NEXT 15 ROWS ONLY", null);
            IsEqual(ep3, result3);

            var result4 = ShardingTableManager<DemoTable>.SecondarySearchPaging(15, 2, "DemoTable", "AddTime", "asc","  AddTime>@AddTime ",new{AddTime= "2020-11-17 08:54:12" });
            var ep4 = DapperHelper.QueryList<DemoTable>(
                $"SELECT * FROM dbo.DemoTable where  AddTime>@AddTime ORDER BY AddTime  OFFSET 15 ROWS FETCH NEXT 15 ROWS ONLY", new { AddTime = "2020-11-17 08:54:12" });
            IsEqual(ep4, result4);


        }

        [TestMethod]
        public void TestGetPageEntities()
        {
            //查第一个表
            var result1 = ShardingTableManager<DemoTable>.GetPageEntities(1000, 1, "*", "DemoTable", "", "AddTime", "asc", "Id") as List<DemoTable>;
            var sql1 = ShardingTableManager<DemoTable>.GetPageSql(1000, 1, "*", "DemoTable0", "", "AddTime", "asc",
                "Id");
            var epR1 = DapperHelper.QueryList<DemoTable>(sql1, null);
            IsEqual(epR1, result1);
         
            //查前两个表
            var result2 = ShardingTableManager<DemoTable>.GetPageEntities(30000, 1, "*", "DemoTable", "", "AddTime", "asc", "Id")
                    as List<DemoTable>;
            var epR2 = DapperHelper.QueryList<DemoTable>("select * from DemoTable0 order by AddTime asc", null);
            epR2.AddRange(DapperHelper.QueryList<DemoTable>("select * from DemoTable1  order by AddTime asc  offset 0 rows fetch next 10000 rows only", null));
            IsEqual(epR2, result2);

            //查前3个表
            var result3 = ShardingTableManager<DemoTable>.GetPageEntities(50000, 1, "*", "DemoTable", "", "AddTime", "asc", "Id")
                as List<DemoTable>;
            var epR3 = DapperHelper.QueryList<DemoTable>("select * from DemoTable0 order by AddTime asc", null);
            epR3.AddRange(DapperHelper.QueryList<DemoTable>("select * from DemoTable1  order by AddTime asc  offset 0 rows fetch next 20000 rows only", null));
            epR3.AddRange(DapperHelper.QueryList<DemoTable>("select * from DemoTable2  order by AddTime asc  offset 0 rows fetch next 10000 rows only", null));
            IsEqual(epR3, result3);
           
            //第一个表部分+第二个表部分 从第二页开始
            var result4 = ShardingTableManager<DemoTable>.GetPageEntities(15000, 2, "*", "DemoTable", "", "AddTime", "asc", "Id") as List<DemoTable>;

            var epR4 = DapperHelper.QueryList<DemoTable>("select * from DemoTable0 order by AddTime asc OFFSET 15000 rows fetch next 15000 rows only", null);
            epR4.AddRange(DapperHelper.QueryList<DemoTable>("select * from DemoTable1 order by AddTime asc OFFSET 0 rows fetch next 10000 rows only", null));
            IsEqual(epR4, result4);

            //第二个表部分+第三个表部分    
            var result5 = ShardingTableManager<DemoTable>.GetPageEntities(300, 134, "*", "DemoTable", "", "AddTime", "asc", "Id") as List<DemoTable>;
            var epR5 = DapperHelper.QueryList<DemoTable>("select * from DemoTable1 order by AddTime asc OFFSET 19900 rows fetch next 300 rows only", null);
            epR5.AddRange(DapperHelper.QueryList<DemoTable>("select * from DemoTable2 order by AddTime asc OFFSET 0 rows fetch next 200 rows only", null));
            IsEqual(epR5, result5);

            var result6= ShardingTableManager<DemoTable>.GetPageEntities(300, 300, "*", "DemoTable", "", "AddTime", "asc", "Id") as List<DemoTable>;
        }

        private void IsEqual(IEnumerable<DemoTable> ep,IEnumerable<DemoTable> re) {
            Assert.AreEqual(ep.Count(), re.Count());
            Assert.AreEqual(ep.First().Id, re.First().Id);
            Assert.AreEqual(ep.Last().Id, re.Last().Id);
        }
    }
}
