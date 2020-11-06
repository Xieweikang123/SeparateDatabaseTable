﻿using System;
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
    public class ShardingTableManager
    {
        /// <summary>
        /// 要分表的表前缀
        /// </summary>
        public static string tableNamePrefix;

        private static readonly int eachTableSize = 2000;


        public static IEnumerable<TEntity> Query<TEntity>(string tableNamePrefix, string sql, object obj)
        {
            var allShardingTableNameList = GetAllShardingTableNames(tableNamePrefix);
            var resultList = new List<TEntity>();
            foreach (var tableName in allShardingTableNameList) {
                sql = $"select * from {tableName}";
                resultList.AddRange( DapperHelper.QueryList<TEntity>(sql,null));
            }

            return resultList;
        }
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