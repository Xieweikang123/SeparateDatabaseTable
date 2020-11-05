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
    public class SeparateTableManager
    {
        /// <summary>
        /// 要分表的表前缀
        /// </summary>
        public static string tableNamePrefix;

        //private static string _currentInsertTableName=string.Empty;
        ///// <summary>
        ///// 当前插入的表名
        ///// </summary>
        //public  static string CurrentInsertTableName {
        //    get {
        //        if (string.IsNullOrWhiteSpace(_currentInsertTableName)) {
        //            _currentInsertTableName = GetLastInsertTableName();
        //            return _currentInsertTableName;
        //        }
        //        return _currentInsertTableName;
        //    }
        //}

        /// <summary>
        /// 向数据库插入数据并判断是否需要分表
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="sql"></param>
        /// <param name="entity">要插入的实体</param>
        /// <param name="tableName">表名前缀</param>
        /// <param name="tableMaxSize">每个表最大条数</param>
        /// <returns></returns>
        public static int InsertToSeparateTable<TEntity>(string insertValueParam, TEntity entity, string tableNamePrefix, int tableMaxSize) {
            
            var tableName = GetLastInsertTableName(tableNamePrefix);

            var sql = $"insert into {tableName} values({insertValueParam})";
            var result = DapperHelper.Insert(sql, entity);

            //获取条数
            var totalCount=(int)SqlHelper.ExecuteScalar($"select count(1) from {tableName}");
            //达到最大值，进行分表
            if (totalCount >= tableMaxSize) {
                var newTableName = tableNamePrefix +  (Convert.ToInt32( tableName.Replace(tableNamePrefix, string.Empty))+1);

                //创建一个新表
                SqlHelper.ExecuteNonQuery($"SELECT* INTO {newTableName} FROM {tableNamePrefix}0 WHERE 1 = 2");
                //SELECT* INTO DemoTable_3 FROM DemoTable_0 WHERE 1 = 2
            }

            return result;
        }
        ///// <summary>
        ///// 获取表后缀
        ///// </summary>
        ///// <param name="tableName"></param>
        ///// <param name="tableNamePrefix"></param>
        ///// <returns></returns>
        //private static string GetTableSuffix(string tableName,string tableNamePrefix) {
        //    return tableName.Replace(tableNamePrefix, string.Empty);
        //}

        /// <summary>
        /// 通过表前缀获取最后一个分表表名
        /// </summary>
        /// <param name="tableNamePrefix">表前缀</param>
        /// <returns></returns>
        public static string GetLastInsertTableName(string tableNamePrefix)
        {

            var table = SqlHelper.ExecuteDataTable("select * from sysobjects where xtype='U'");
            var separateTableSuffixList = new List<int>();
            foreach (DataRow row in table.Rows)
            {
                var tableName = row["name"].ToString();
                if (tableName.Contains(tableNamePrefix))
                {
                    separateTableSuffixList.Add(int.Parse(tableName.Replace(tableNamePrefix, string.Empty)));
                }
            }
            separateTableSuffixList.Sort();

            return tableNamePrefix + separateTableSuffixList.Last();
        }



    }
}
