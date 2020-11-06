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
    /// <summary>
    /// 分表方式
    /// </summary>
    public enum ShardingType
    {
        Count,//通过数量
       //通过年月日分表 
        Year,
        Month,
        Day
    }
    public class SeparateTableManager
    {
        /// <summary>
        /// 要分表的表前缀
        /// </summary>
        public static string tableNamePrefix;

        private static readonly int eachTableSize = 100000;

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


        //public static bool InserToSeparateTableByDateTime<TEntity>(string insertValueParam,TEntity entity,string tableNamePrefix) {


        //}

       
        /// <summary>
        /// 向数据库插入数据并判断是否需要分表
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="sql"></param>
        /// <param name="entity">要插入的实体</param>
        /// <param name="tableName">表名前缀</param>
        /// <param name="shardingType">分表方式</param>
        /// <returns></returns>
        public static int InsertToSeparateTable<TEntity>(string insertValueParam, TEntity entity, string tableNamePrefix,ShardingType shardingType ) {

            var tableName = string.Empty;
            //if (shardingType == ShardingType.Count) {
            //} else {

            //    tableName=tableNamePrefix
            //}

            //按时间分表，格式: 表前缀_2020_01_01
            switch (shardingType) {
                case ShardingType.Count:
                    tableName = GetLastInsertTableName(tableNamePrefix);
                    break;
                case ShardingType.Year:
                case ShardingType.Month:
                case ShardingType.Day:
                    tableName = GetTableSuffixNameByDateTime(tableNamePrefix,DateTime.Now, shardingType);
                    break;

                    //case ShardingType.Year:
                    //    //var suffix = DateTime.Now.Year;
                    //    tableName = tableNamePrefix + DateTime.Now.Year;
                    //    break;
                    //case ShardingType.Month:
                    //    tableName = tableNamePrefix + DateTime.Now.Year + "_" + DateTime.Now.Month;
                    //    break;
                    //case ShardingType.Day:
                    //    tableName = tableNamePrefix + DateTime.Now.Year + "_" + DateTime.Now.Month+"_"+DateTime.Now.Day;
                    //    break;
            }
            //数据表是否存在
            if (!IsTableExist(tableName)) {
                //不存在，创建
                SqlHelper.ExecuteNonQuery($"SELECT* INTO {tableName} FROM {tableNamePrefix} WHERE 1 = 2");
            }

            var sql = $"insert into {tableName} values({insertValueParam})";

            //进行插入
            var result = DapperHelper.Insert(sql, entity);

            //如果是通过数量分表，查看是否需要创建新表
            if(shardingType == ShardingType.Count)
            {
                //获取条数
                var totalCount = (int)SqlHelper.ExecuteScalar($"select count(1) from {tableName}");
                //达到最大值，进行分表
                if (totalCount >= eachTableSize)
                {
                    var newTableName = tableNamePrefix + (Convert.ToInt32(tableName.Replace(tableNamePrefix, string.Empty)) + 1);

                    //创建一个新表
                    SqlHelper.ExecuteNonQuery($"SELECT* INTO {newTableName} FROM {tableNamePrefix}0 WHERE 1 = 2");
                }
            }

            return result;
        }

        public static string GetTableSuffixNameByDateTime(string tableNamePrefix, DateTime dateTime,ShardingType shardingType) {
            var tableName = string.Empty;
            switch (shardingType) {
                case ShardingType.Year:
                    //var suffix = DateTime.Now.Year;
                    tableName = tableNamePrefix + DateTime.Now.Year;
                    break;
                case ShardingType.Month:
                    tableName = tableNamePrefix + DateTime.Now.Year + "_" + DateTime.Now.Month;
                    break;
                case ShardingType.Day:
                    tableName = tableNamePrefix + DateTime.Now.Year + "_" + DateTime.Now.Month + "_" + DateTime.Now.Day;
                    break;
            }

            return tableName;
        }

        private static bool IsTableExist(string tableName) {

            string sql = $" select TABLE_NAME from INFORMATION_SCHEMA.TABLES where   TABLE_NAME='{tableName}' ";
            var result = SqlHelper.ExecuteScalar(sql);

            return result!=null;
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
        private static string GetLastInsertTableName(string tableNamePrefix)
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
