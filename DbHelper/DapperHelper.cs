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

     
        public static int Insert<TEntity>(string sql, IEnumerable<TEntity> entities)
        {
            using (var connection = new SqlConnection(connectionString))
            {

                var result = connection.Execute(sql, entities);
                return result;
            }
        }

        //public static List<TEntity> QueryList<TEntity>(string sql,TEntity entity) {
        //    using (var connection = new SqlConnection(connectionString))
        //    {
        //        //connection.Query<Person>("select * from Person").ToList();
        //        return connection.Query<TEntity>(sql, entity).ToList();

        //        //return connection.Query<TEntity>("select * from Person where id=@ID", entity).ToList();
        //    }
        //}

        public static List<TEntity> QueryList<TEntity>(string sql, object obj)
        {
            using (var connection = new SqlConnection(connectionString))
            {
                //connection.QueryAsync()
                //connection.Query<Person>("select * from Person").ToList();
                return connection.Query<TEntity>(sql, obj).ToList();

                //return connection.Query<TEntity>("select * from Person where id=@ID", entity).ToList();
            }
        }
        public static Task< IEnumerable<TEntity>> QueryListAsync<TEntity>(string sql, object obj)
        {
            using (var connection = new SqlConnection(connectionString))
            {
                return  connection.QueryAsync<TEntity>(sql, obj);
            }
        }


    }
}
