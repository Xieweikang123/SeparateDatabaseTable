using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using SeparateDataHelper;

namespace DbHelper
{
    public class SeparateTableManager {
        /// <summary>
        /// 要分表的表前缀
        /// </summary>
        public static string tableNamePrefix;

        private static string _currentInsertTableName=string.Empty;
        /// <summary>
        /// 当前插入的表名
        /// </summary>
        public  static string CurrentInsertTableName {
            get {
                if (string.IsNullOrWhiteSpace(_currentInsertTableName)) {
                    _currentInsertTableName = GetLastInsertTableName();
                    return _currentInsertTableName;
                }
                return _currentInsertTableName;
            }
        }
        private static string GetLastInsertTableName() {

            var table = SqlHelper.ExecuteDataTable("select * from sysobjects where xtype='U'");
            var separateTableSuffixList = new List<int>();
            foreach (DataRow row in table.Rows) {
                var tableName = row["name"].ToString();
                if (tableName.Contains(tableNamePrefix)) {
                    separateTableSuffixList.Add(int.Parse(tableName.Replace(tableNamePrefix,string.Empty)));
                }
            }
            separateTableSuffixList.Sort();

            return tableNamePrefix+separateTableSuffixList.Last();
        }



    }
}
