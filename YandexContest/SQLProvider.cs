using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace YandexContest
{
    static class Query
    {
        private static readonly string connString =
            "data source=\"C: \\Users\\Master\\Documents\\Visual Studio 2017\\Projects\\YandexContest\\YCDB.db\"";
        private static readonly SQLiteConnection conn = new SQLiteConnection(connString);

        private static SQLiteCommand GenerateCommand(string query = "")
        {
            var cmd = conn.CreateCommand();
            if(query != "")
            cmd.CommandText = query;
            return cmd;
        }

        public static DataTable ExecuteQuery(string query)
        {
            var dt = new DataTable();
            try
            {
                conn.Open();
                var cmd = GenerateCommand(query);
                var sqliteDA = new SQLiteDataAdapter(cmd);
                sqliteDA.Fill(dt);
            }
            catch (SQLiteException e)
            {
                Console.WriteLine(e.Message);
                return null;
            }
            finally
            {
                conn.Close();
            }
            return dt;
        }

        public static DataTable ExecuteQuery(SQLiteCommand cmd)
        {
            var dt = new DataTable();
            try
            {
                conn.Open();
                cmd.Connection = conn;
                var sqliteDA = new SQLiteDataAdapter(cmd);
                sqliteDA.Fill(dt);
            }
            catch (SQLiteException e)
            {
                Console.WriteLine(e.Message);
                return null;
            }
            finally
            {
                conn.Close();
            }
            return dt;
        }
    }
}
