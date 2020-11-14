using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
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
    public class ShardingTableManager<TEntity>
    {
        /// <summary>
        /// 要分表的表前缀
        /// </summary>
        public static string tableNamePrefix;

        private static readonly int eachTableSize = 2000;
        /// <summary>
        /// 查询结果集
        /// </summary>
        private static List<TEntity> resultEntities;
        /// <summary>
        /// 将所有分表汇总到一起
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="tableNamePrefix"></param>
        /// <param name="sql"></param>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static IEnumerable<TEntity> QueryAll(string tableNamePrefix, string whereSql = "", object obj = null, string filesSql = "*")
        {

            //初始化结果集
            var resultEntities1 = new List<TEntity>();
            //获取所有分表集合
            var allShardingTableNameList = GetAllShardingTableNames(tableNamePrefix);

            var uniAllSql = string.Empty;
            var sqlList = new List<string>();
            foreach (var tableName in allShardingTableNameList)
            {
                sqlList.Add($" select {filesSql} from {tableName} {whereSql} ");
                //uniAllSql += $" select {filesSql} from {tableName} {whereSql} union all ";
            }

            uniAllSql = string.Join(" union all ", sqlList);

            resultEntities1 = DapperHelper.QueryList<TEntity>(uniAllSql, obj);
            //var manualResetEventList = new List<ManualResetEvent>();

            //for (var i = 0; i < allShardingTableNameList.Count(); i++)
            //{
            //    var mre = new ManualResetEvent(false);
            //    manualResetEventList.Add(mre);
            //    //ThreadPool.QueueUserWorkItem(ThreadMethod, new { id = i, url = "www", mre });
            //   var  sql = $"select {filesSql} from {allShardingTableNameList.ElementAt(i)}";
            //   if (!string.IsNullOrWhiteSpace(whereSql)) {
            //       sql += $"  {whereSql}";
            //   }

            //    ThreadPool.QueueUserWorkItem(ThreadMethod, new { sql, mre, resultEntities1,obj });
            //}
            ////等待所有线程执行完毕
            //WaitHandle.WaitAll(manualResetEventList.ToArray());

            return resultEntities1;
        }
        /// <summary>
        /// 分页查询分表
        /// </summary>
        /// <param name="pageSize">每页大小</param>
        /// <param name="currentPage">当前页，从1开始</param>
        /// <param name="columns">要筛选的列</param>
        /// <param name="tableNamePrefix">分表前缀</param>
        /// <param name="whereStr">查询条件</param>
        /// <param name="orderColumn">排序的字段</param>
        /// <param name="orderType">排序类型 desc asc</param>
        /// <param name="pkColumn">主键</param>
        /// <returns></returns>
        public static IEnumerable<TEntity> GetPageEntities(Int32 pageSize, Int32 currentPage, String columns, String tableNamePrefix, String whereStr, String orderColumn, String orderType, String pkColumn)
        {
            //获取所有分表集合
            var allShardingTableNameList = GetAllShardingTableNames(tableNamePrefix);
            //总偏移量
            var totalPageOffset = (currentPage - 1) * pageSize;
            //当前已遍历表的行数 已偏移量
            var currentSearchCount = 0;
            var sql = string.Empty;
            //循环轮数
            var tableIndex = -1;
            var resultList = new List<TEntity>();
            foreach (var tableName in allShardingTableNameList)
            {
                tableIndex++;

                //当前表行项数 
                var currentTableRowCount = (int)SqlHelper.ExecuteScalar($"select count(1) from {tableName}");
                //已偏移量
                var hasOffsetCount = currentTableRowCount;
                //剩余偏移量
                var residualOffsetCount = totalPageOffset - currentSearchCount;
                residualOffsetCount = residualOffsetCount > 0 ? residualOffsetCount : 0;
                //剩余检索行数
                var residualRowsCount = pageSize - resultList.Count;
                currentSearchCount += currentTableRowCount;
                //已检索表数据量小于分页偏移量，说明此表不在分页范围内，继续
                if (currentSearchCount < totalPageOffset) {
                    continue;
                }

                //此表存在要分页的数据  存在多少，从哪里开始分页  ;
                //分两种情况：1 初次分表  2 继续分表
                sql = $"select * from {tableName} order by {orderColumn} {orderType} OFFSET {residualOffsetCount} rows fetch next {residualRowsCount} rows only";
                var currentResult = DapperHelper.QueryList<TEntity>(sql, null);
                resultList.AddRange(currentResult);
                //检索够了
                if (pageSize == resultList.Count) {
                    return resultList;
                }
            }

            return resultList;
        }


        /// <summary>
        /// 获取分页Sql  (适用sql server 2012版本以上)
        /// </summary>
        /// <param name="pageSize">分页大小</param>
        /// <param name="currentPage">当前第几页</param>
        /// <param name="columns">要查询的列</param>
        /// <param name="tableName">表名称</param>
        /// <param name="whereStr">条件语句(要以and开头)</param>
        /// <param name="orderColumn">排序字段</param>
        /// <param name="orderType">排序类型(desc, asc)</param>
        /// <param name="pkColumn">主键</param>
        /// <returns></returns>
        public static string GetPageSql(Int32 pageSize, Int32 currentPage, String columns, String tableName, String whereStr, String orderColumn, String orderType, String pkColumn)
        {
            var sqlStr = $"select {columns} from {tableName} ";
            if (!string.IsNullOrWhiteSpace(whereStr))
            {
                sqlStr += $" where { whereStr}";
            }
            //没有排序字段,用id排序
            if (string.IsNullOrWhiteSpace(orderColumn))
            {
                sqlStr += $" order by {pkColumn} {orderType} ";
            }
            else
            {
                sqlStr += $" order by {orderColumn} {orderType} ";
            }
            //当前第几页   OFFSET 0*10 ROWS
            sqlStr += $" offset {(currentPage - 1) * pageSize} rows";
            sqlStr += $" fetch next {pageSize} rows only";
            return sqlStr;
        }
        /// <summary>
        /// 统计所有分表里数据行项总数
        /// </summary>
        /// <param name="tableNamePrefix"></param>
        /// <returns></returns>
        public static int QueryCount(string tableNamePrefix)
        {

            var allCount = 0;
            //获取所有分表集合
            var allShardingTableNameList = GetAllShardingTableNames(tableNamePrefix);
            foreach (var tableName in allShardingTableNameList)
            {
                var sql = $"select count(1) from {tableName}";
                allCount += (int)SqlHelper.ExecuteScalar(sql);
            }

            return allCount;
        }

        //public static void ThreadMethod(object parameter) //方法内可以有参数，也可以没有参数
        //{
        //    var sql = ((dynamic)parameter).sql;

        //    var obj=((dynamic)parameter).obj;

        //    var resultEntities1 = ((dynamic)parameter).resultEntities1 as List<TEntity>;
        //    lock (resultEntities1)
        //    {
        //        resultEntities1.AddRange(DapperHelper.QueryList<TEntity>(sql, obj));
        //    }

        //    Console.WriteLine($"线程{sql}执行完毕 ");
        //    ((dynamic)parameter).mre.Set();
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
        public static int InsertToSeparateTable<TEntity>(string insertValueParam, TEntity entity, string tableNamePrefix, ShardingType shardingType)
        {

            var tableName = string.Empty;
            //if (shardingType == ShardingType.Count) {
            //} else {

            //    tableName=tableNamePrefix
            //}

            //按时间分表，格式: 表前缀_2020_01_01
            switch (shardingType)
            {
                case ShardingType.Count:
                    tableName = GetLastInsertTableName(tableNamePrefix);
                    break;
                case ShardingType.Year:
                case ShardingType.Month:
                case ShardingType.Day:
                    tableName = GetTableSuffixNameByDateTime(tableNamePrefix, DateTime.Now, shardingType);
                    break;
            }
            //数据表是否存在
            if (!IsTableExist(tableName))
            {
                //不存在，创建
                SqlHelper.ExecuteNonQuery($"SELECT* INTO {tableName} FROM {tableNamePrefix} WHERE 1 = 2");
            }

            var sql = $"insert into {tableName} values({insertValueParam})";

            //进行插入
            var result = DapperHelper.Insert(sql, entity);

            //如果是通过数量分表，查看是否需要创建新表
            if (shardingType == ShardingType.Count)
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
        /// <summary>
        /// 指定日期查询对应分表
        /// </summary>
        /// <param name="tableNamePrefix"></param>
        /// <param name="dateTime"></param>
        /// <param name="shardingType"></param>
        /// <returns></returns>
        public static string GetTableSuffixNameByDateTime(string tableNamePrefix, DateTime dateTime, ShardingType shardingType)
        {
            var tableName = string.Empty;
            switch (shardingType)
            {
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

        private static bool IsTableExist(string tableName)
        {

            string sql = $" select TABLE_NAME from INFORMATION_SCHEMA.TABLES where   TABLE_NAME='{tableName}' ";
            var result = SqlHelper.ExecuteScalar(sql);

            return result != null;
        }

        /// <summary>
        /// 通过表前缀获取最后一个分表表名
        /// </summary>
        /// <param name="tableNamePrefix">表前缀</param>
        /// <returns></returns>
        private static string GetLastInsertTableName(string tableNamePrefix)
        {
            var allshardingTableNameList = GetAllShardingTableNames(tableNamePrefix);


            //var table = SqlHelper.ExecuteDataTable("select * from sysobjects where xtype='U'");
            var separateTableSuffixList = new List<int>();
            if (allshardingTableNameList.Count() == 0)
            {
                separateTableSuffixList.Add(0);
            }
            else
            {
                foreach (var tableName in allshardingTableNameList)
                {
                    separateTableSuffixList.Add(int.Parse(tableName.Replace(tableNamePrefix, string.Empty)));
                }
                separateTableSuffixList.Sort();
            }

            return tableNamePrefix + separateTableSuffixList.Last();
        }
        /// <summary>
        /// 获取所有分表名
        /// </summary>
        /// <returns></returns>
        private static IEnumerable<string> GetAllShardingTableNames(string tableNamePrefix)
        {
            var result = new List<string>();
            var table = SqlHelper.ExecuteDataTable("select * from sysobjects where xtype='U'");
            var separateTableSuffixList = new List<int>();
            foreach (DataRow row in table.Rows)
            {
                var tableName = row["name"].ToString();
                if (tableName.Contains(tableNamePrefix) && tableName != tableNamePrefix)
                {
                    result.Add(tableName);
                    //separateTableSuffixList.Add(int.Parse(tableName.Replace(tableNamePrefix, string.Empty)));
                }
            }

            return result;
        }



    }
}
