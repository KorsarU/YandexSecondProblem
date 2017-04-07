using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace YandexContest
{
    static class Query
    {
        private static string connString;
        private static SQLiteConnection conn;

        public static bool SetConnection(String connectionstring = "")
        {
            if (connectionstring == "")
            {
                //data source="C:\Users\Master\Documents\Visual Studio 2017\Projects\YandexSecondProblem\YCDB.db"
                DirectoryInfo di = new DirectoryInfo("./");
                var result = "";
                while (String.IsNullOrEmpty(result))
                {

                    var dbFile = di.GetFiles("YCDB.db");
                    if (dbFile.Length == 0)
                    {
                        di = (Directory.GetParent(di.FullName));
                    }
                    else
                    {
                        result = dbFile[0].FullName;
                    }
                }
                connString = "data source = " + result;
            }
            else
            {
                connString = connectionstring;
            }
            conn = new SQLiteConnection(connString);
            try
            {
                conn.Open();
                conn.Close();
                return true;
            }
            catch (SQLiteException e)
            {
                Console.WriteLine(e.Message);
            }
            return false;
        }

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
