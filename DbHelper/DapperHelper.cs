using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;

namespace DbHelper
{
    public class DapperHelper
    {
        private static readonly string connectionString = ConfigurationManager.ConnectionStrings["SQLConnectionString"].ToString();
        //public static string GetSqlConnectionString()
        //{
        //    return ConfigurationManager.ConnectionStrings["SQLConnectionString"].ToString();
        //}

        /// <summary>
        /// var result = connection.Execute("Insert into Users values (@UserName, @Email, @Address)",  new { UserName = "jack", Email = "380234234@qq.com", Address = "上海" });
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="paramObj"></param>
        /// <returns></returns>
        public static int Insert(string sql, object paramObj)
        {
            using (var connection = new SqlConnection(connectionString))
            {
                return connection.Execute(sql, paramObj);
            }
        }

        public static int Insert<TEntity>(string sql, TEntity entity)
        {
            using (var connection = new SqlConnection(connectionString))
            {

                var result = connection.Execute(sql, entity);

                return result;
            }
        }
    }
}
