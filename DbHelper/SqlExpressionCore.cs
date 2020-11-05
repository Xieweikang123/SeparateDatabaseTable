﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DbHelper
{
    /// <summary>
    /// Expression核心
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class SqlExpressionCore<T>
    {
        private SqlGenerate sqlGenerate = new SqlGenerate();
        /// <summary>
        /// 脚本
        /// </summary>
		public string Script { get { return sqlGenerate.ToString(); } }
        /// <summary>
        /// 参数
        /// </summary>
        public Dictionary<string, object> DbParams { get { return sqlGenerate.DbParams; } }

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="dbType"></param>
        /// <param name="tableName"></param>
        public SqlExpressionCore(DatabaseType dbType, string tableName = "")
        {
            if (string.IsNullOrEmpty(tableName))
                tableName = typeof(T).Name;

            sqlGenerate.DatabaseType = dbType;
            sqlGenerate.TableName = tableName.ParamSql(dbType);
        }

        /// <summary>
        /// 清除
        /// </summary>
        public void Clear()
        {
            this.sqlGenerate.Clear();
        }

        /// <summary>
        /// 查询
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="orderBy">排序字段</param>
        /// <returns></returns>
        public SqlExpressionCore<T> Select(Expression<Func<T, object>> expression = null, string orderBy = "")
        {
            var sql = $"select {{0}}{Environment.NewLine}from {sqlGenerate.TableName}";
            var fields = string.Empty;

            if (expression == null)
                fields = "*";
            else
            {
                SqlExpressionProvider.Select(expression, sqlGenerate);
                fields = sqlGenerate.SelectFieldsStr;
            }

            sqlGenerate.Sql.AppendFormat(sql, fields);
            return this;
        }

        /// <summary>
        /// Where条件
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        public SqlExpressionCore<T> Where(Expression<Func<T, bool>> expression)
        {
            if (expression == null)
                return this;

            sqlGenerate += $"{Environment.NewLine}where";
            SqlExpressionProvider.Where(expression, sqlGenerate);
            return this;
        }

        /// <summary>
        /// OrderBy
        /// </summary>
        /// <param name="orderBy"></param>
        /// <returns></returns>
        public SqlExpressionCore<T> OrderBy(string orderBy)
        {
            if (string.IsNullOrEmpty(orderBy))
            {
                var property = typeof(T).GetProperty<KeyAttribute>();
                if (property == null)
                    property = typeof(T).GetProperties()[0];
                orderBy = $"order by {property.Name} desc";
            }

            if (!orderBy.StartsWith("order by"))
                orderBy = $"order by {orderBy}";

            switch (sqlGenerate.DatabaseType)
            {
                case DatabaseType.SqlServer: // 2012版本支持 内部数据库版本706 【select DATABASEPROPERTYEX('master','version')】
                    sqlGenerate.Sql.Replace("select", $"select row_number() over({orderBy}) as RowNumber,");
                    break;
                case DatabaseType.GteSqlServer2012:
                    sqlGenerate += $"{Environment.NewLine}{orderBy}";
                    break;
                case DatabaseType.MySql:
                    sqlGenerate += $"{Environment.NewLine}{orderBy}";
                    break;
                case DatabaseType.SQLite:
                    sqlGenerate += $"{Environment.NewLine}{orderBy}";
                    break;
            }
            return this;
        }

        /// <summary>
        /// Limit
        /// </summary>
        /// <param name="page"></param>
        /// <param name="rows"></param>
        /// <returns></returns>
        public SqlExpressionCore<T> Limit(int page, int rows)
        {
            var skip = (page - 1) * rows;
            switch (sqlGenerate.DatabaseType)
            {
                case DatabaseType.SqlServer:
                    sqlGenerate.Sql = new StringBuilder($"SELECT it.* FROM ({sqlGenerate.Sql}) it where it.RowNumber > {skip} AND it.RowNumber <= {page * rows}");
                    break;
                case DatabaseType.GteSqlServer2012:
                    sqlGenerate += $" OFFSET {skip} ROW FETCH NEXT {rows} rows only";
                    break;
                case DatabaseType.MySql:
                    sqlGenerate += $" limit {skip}, {rows}";
                    break;
                case DatabaseType.SQLite:
                    sqlGenerate += $" limit {rows} offset {skip}";
                    break;
                default:
                    break;
            }
            return this;
        }

        /// <summary>
        /// Offset
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="size"></param>
        /// <returns></returns>
        public SqlExpressionCore<T> Offset(int offset, int size)
        {
            switch (sqlGenerate.DatabaseType)
            {
                case DatabaseType.SqlServer:
                    sqlGenerate.Sql = new StringBuilder($"SELECT it.* FROM ({sqlGenerate.Sql}) it where it.RowNumber > {offset} AND it.RowNumber <= {offset + size}");
                    break;
                case DatabaseType.GteSqlServer2012:
                    sqlGenerate += $" OFFSET {offset} ROW FETCH NEXT {size} rows only";
                    break;
                case DatabaseType.MySql:
                    sqlGenerate += $" limit {offset}, {size}";
                    break;
                case DatabaseType.SQLite:
                    sqlGenerate += $" limit {size} offset {offset}";
                    break;
                default:
                    break;
            }
            return this;
        }

        /// <summary>
        /// 最大
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        public SqlExpressionCore<T> Max(Expression<Func<T, object>> expression)
        {
            sqlGenerate.Clear();
            SqlExpressionProvider.Max(expression, sqlGenerate);
            return this;
        }

        /// <summary>
        /// 最小值
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        public SqlExpressionCore<T> Min(Expression<Func<T, object>> expression)
        {
            sqlGenerate.Clear();
            SqlExpressionProvider.Min(expression, sqlGenerate);
            return this;
        }

        /// <summary>
        /// 平均值
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        public SqlExpressionCore<T> Avg(Expression<Func<T, object>> expression)
        {
            sqlGenerate.Clear();
            SqlExpressionProvider.Avg(expression, sqlGenerate);
            return this;
        }

        /// <summary>
        /// 行数
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        public SqlExpressionCore<T> Count(Expression<Func<T, object>> expression = null)
        {
            sqlGenerate.Clear();
            if (expression == null)
                sqlGenerate.Sql.Append($"select count(*) from {sqlGenerate.TableName}");
            else
                SqlExpressionProvider.Count(expression, this.sqlGenerate);

            return this;
        }

        /// <summary>
        /// 总计
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        public SqlExpressionCore<T> Sum(Expression<Func<T, object>> expression)
        {
            sqlGenerate.Clear();
            SqlExpressionProvider.Sum(expression, sqlGenerate);
            return this;
        }

        /// <summary>
        /// 删除
        /// </summary>
        /// <returns></returns>
        public SqlExpressionCore<T> Delete()
        {
            sqlGenerate.Clear();
            sqlGenerate += $"delete from {sqlGenerate.TableName}";
            return this;
        }

        /// <summary>
        /// 修改
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        public SqlExpressionCore<T> Update(Expression<Func<object>> expression = null)
        {
            sqlGenerate.Clear();
            sqlGenerate += $"update {sqlGenerate.TableName} set ";
            SqlExpressionProvider.Update(expression, sqlGenerate);
            return this;
        }
    }
}
