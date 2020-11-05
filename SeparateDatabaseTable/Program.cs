using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using  DbHelper;

namespace SeparateDatabaseTable
{
    class Program
    {
        static void Main(string[] args) {
            SeparateTableManager.tableNamePrefix = "DemoTable_";

            var tablename = SeparateTableManager.CurrentInsertTableName;
        }
    }
}
