using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace YandexContest
{
    class Program
    {
        static void Main(string[] args)
        {
            var path = "./Data.txt";
            if (args.Length == 1) path = "./"+args[0];
            var readResult = ReadDataFile(path);
            if(readResult!=null)
            LoadDataSet(readResult);
            //RunCommandInterface();
        }

        private static void LoadDataSet(IEnumerable<Tuple<DateTime, float, int>> readResult)
        {
            
            foreach (var tuple in readResult)
            {
                try
                {
                    var cmd = new SQLiteCommand
                    {
                        CommandText = ("INSERT INTO [order]([dt],[product_id],[amount])" +
                                       "VALUES(@dt,@p_id,@amount)")
                    };
                    cmd.Parameters.Add("@dt", DbType.DateTime);
                    cmd.Parameters.Add("@p_id", DbType.Int32);
                    cmd.Parameters.Add("@amount", DbType.Decimal);
                    cmd.Parameters["@dt"].Value = tuple.Item1;
                    cmd.Parameters["@p_id"].Value = tuple.Item3;
                    cmd.Parameters["@amount"].Value = tuple.Item2;
                    Query.ExecuteQuery(cmd);
                }
                catch (SQLiteException e)
                {
                    Console.WriteLine(e.Message);
                }
            }
        }

        private static void RunCommandInterface()
        {
            throw new NotImplementedException();
        }

        private static IEnumerable<Tuple<DateTime, float, int>> ReadDataFile(string path)
        {
            StreamReader fstr;
            try
            {
                var f = new FileInfo(path);
                if (!IsExtentionCorrect(f.Extension))
                {
                    Console.WriteLine("Wrong file extention");
                    return null;
                }
                fstr = new StreamReader(f.FullName);
            }
            catch (Exception e)
            {
                Console.WriteLine("Read file: " + e.Message);
                return null;
            }
            var result = new List<Tuple<DateTime, float, int>>();
            var orderList = new Dictionary<string, int>();
            var line = fstr.ReadLine();
            if (!string.IsNullOrEmpty(line))
            {
                var buf = line.Split('\t');
                var i = 0;
                foreach (var s in buf)
                {
                    orderList.Add(s.ToLower().Trim(),i);
                    i++;
                }
            }
            else return null;
            var lineNum = 0;
            while (!fstr.EndOfStream)
            {
                var readLine = fstr.ReadLine();
                if (string.IsNullOrEmpty(readLine)) continue;
                var buf = readLine.Split('\t');
                if (buf.Length != orderList.Keys.Count)
                    Console.WriteLine("Line " + lineNum + ": Miss qunatity of values");
                try
                {
                    var dt = DateTime.Parse(buf[orderList["dt"]]);
                    var amount = float.Parse(buf[orderList["amount"]]);
                    var p_id = int.Parse(buf[orderList["product_id"]]);
                    result.Add(Tuple.Create(dt, amount, p_id));
                }
                catch (InvalidCastException e)
                {
                    Console.WriteLine("Line " + lineNum + ": " + e.Message);
                }
                finally
                {
                    lineNum++;
                }
            }
            fstr.Close();
            return result;
        }

        private static bool IsExtentionCorrect(string s)
        {
            return (s == ".tsv" || s == ".txt") ;
        }

        public DataTable Execute(string query)
        {
            var result = Query.ExecuteQuery(query);
            if (result == null) Console.WriteLine("0 rows");
            return result;
        }

        public void PrintResult(DataTable result)
        {

            foreach (DataRow resultRow in result.Rows)
            {
                for (var i = 0; i < result.Columns.Count; i++)
                {
                    Console.Write(resultRow[i].ToString() + ' ');
                }
                Console.WriteLine();
            }
        }


    }
}
