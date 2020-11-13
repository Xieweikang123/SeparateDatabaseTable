using System;
using System.Collections.Generic;
using System.Linq;
using DbHelper;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SeparateDatabaseTable.Models;

namespace DBHelperTest
{
    [TestClass]
    public class ShardingTableManagerTest
    {
        [TestMethod]
        public void TestGetPageEntities()
        {
            //查第一个表
            var result1 = ShardingTableManager<DemoTable>.GetPageEntities(1000, 1, "*", "DemoTable", "", "AddTime", "asc", "Id") as List<DemoTable>;
            var sql1= ShardingTableManager<DemoTable>.GetPageSql(1000, 1, "*", "DemoTable0", "", "AddTime", "asc",
                "Id");
            var epR1 = DapperHelper.QueryList<DemoTable>(sql1, null);

            IsEqual(epR1,result1);
            //查前两个表
            var result2 =  ShardingTableManager<DemoTable>.GetPageEntities(30000, 1, "*", "DemoTable", "", "AddTime", "asc", "Id")
                    as List<DemoTable>;
            var epR2 = DapperHelper.QueryList<DemoTable>("select * from DemoTable0 order by AddTime asc", null);
            epR2.AddRange(DapperHelper.QueryList<DemoTable>("select * from DemoTable1  order by AddTime asc  offset 0 rows fetch next 10000 rows only", null));
            IsEqual(epR2,result2);

            //查前3个表
            var result3 = ShardingTableManager<DemoTable>.GetPageEntities(70000, 1, "*", "DemoTable", "", "AddTime", "asc", "Id")
                as List<DemoTable>;
            var epR3 = DapperHelper.QueryList<DemoTable>("select * from DemoTable0 order by AddTime asc", null);
            epR2.AddRange(DapperHelper.QueryList<DemoTable>("select * from DemoTable1  order by AddTime asc  offset 0 rows fetch next 10000 rows only", null));
            IsEqual(epR3, result3);

            ////第一个表部分+第二个表部分 从第二页开始
            //var result3 =
            //    ShardingTableManager<DemoTable>.GetPageEntities(15000, 2, "*", "DemoTable", "", "AddTime", "asc", "Id") as List<DemoTable>;

            //var epR3 = DapperHelper.QueryList<DemoTable>(
            //    "select * from DemoTable0 order by AddTime asc OFFSET 15000 rows fetch next 15000 rows only", null);
            //epR3.AddRange(DapperHelper.QueryList<DemoTable>(
            //    "select * from DemoTable1 order by AddTime asc OFFSET 0 rows fetch next 10000 rows only", null));
            //IsEqual(epR3,result3);
        }

        private void IsEqual(IList<DemoTable> ep,IList<DemoTable> re) {
            Assert.AreEqual(ep.Count, re.Count());
            Assert.AreEqual(ep[0].Id, re[0].Id);
            Assert.AreEqual(ep[ep.Count - 1].Id, re[re.Count - 1].Id);
        }
    }
}
