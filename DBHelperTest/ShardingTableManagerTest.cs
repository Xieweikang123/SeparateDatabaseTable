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
            var result1 = ShardingTableManager<DemoTable>.GetPageEntities(1000, 1, "*", "DemoTable", "", "AddTime", "asc", "Id") as List<DemoTable>;
            var sql1= ShardingTableManager<DemoTable>.GetPageSql(1000, 1, "*", "DemoTable0", "", "AddTime", "asc",
                "Id");
            var epR1 = DapperHelper.QueryList<DemoTable>(sql1, null);


            //Assert.AreEqual(epR1,result1);
            Assert.AreEqual(epR1.Count,result1.Count());
            Assert.AreEqual(epR1[0].Id, result1[0].Id);
            Assert.AreEqual(epR1[epR1.Count-1].Id, result1[result1.Count-1].Id);

        }
    }
}
