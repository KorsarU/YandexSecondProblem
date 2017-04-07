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
            Query.SetConnection();
            Console.OutputEncoding = Encoding.UTF8;
            var path = "./Data.txt";
            if (args.Length == 1) path = "./"+args[0];
            var readResult = ReadDataFile(path);
            if(readResult!=null)
            LoadDataSet(readResult);
            DemoRun();
            //RunCommandInterface();
            Console.ReadKey();
        }



        private static void DemoRun()
        {
            PrintResult(Demo("Demo 1: Select from product table:", "select * from product"));
            PrintResult(Demo("Demo 2: Select NAME, COUNT AND SUM from order table:",
                @"Select product.name as 'продукт', count('order'.product_id) as 'количество', sum(amount) as 'сумма' 
                    from product join 'order' on product.id = 'order'.product_id
                    where 'order'.dt >= date('now','start of month')
                    group by 'order'.product_id"));
            PrintResult(Demo("Demo 3: Select ALL PRODUCTS WHICH WAS ORDERED EXACTLY IN CURRENT MONTH from product table:",
                @"SELECT product.name AS 'продукт', COUNT(o.product_id) AS 'количество', SUM(o.amount) AS 'сумма' 
                    FROM product JOIN 
                        (SELECT * FROM 'order' AS o
					    WHERE o.dt >= DATE('now', 'start of month') 
					    AND 
					    o.product_id NOT IN(
                            SELECT product_id 
                            FROM 'order' 
					        WHERE dt > DATE('now', 'start of month', '-1 month', '-1 day') 
                            AND 
                            dt < DATE('now', 'start of month'))) 
					AS o ON o.product_id = product.id
                    GROUP BY o.product_id"));
            PrintResult(Demo("Demo 4: Select from past and current month distinct from product table:", @"
                            SELECT product.name AS 'продукт',
                                CASE
	                                WHEN o.dt >= date('now','start of month') THEN strftime('[%Y-%m]','now')
	                                WHEN o.dt < date('now','start of month') THEN strftime('[%Y-%m]','now', 'start of month', '-2 day')
                                END 'месяц'
                                FROM product JOIN 
                                (SELECT o1.product_id, o1.dt FROM 'order' AS o1
                                	WHERE o1.dt >= DATE('now', 'start of month') 
                                	AND 
                                	o1.product_id NOT IN(
                                    		SELECT product_id 
                                        	FROM 'order' 
                                			WHERE dt > DATE('now', 'start of month', '-1 month', '-1 day') 
                                        	AND 
                                        	dt < DATE('now', 'start of month')
                                        )
                                UNION
                                SELECT o2.product_id, o2.dt FROM 'order' AS o2
                                		WHERE o2.dt> DATE('now', 'start of month', '-1 month', '-1 day') 
                                        AND 
                                        o2.dt < DATE('now', 'start of month')  
                                		AND 
                                		o2.product_id NOT IN(
                                        		SELECT product_id 
                                        	    FROM 'order' 
                                				WHERE dt >= DATE('now', 'start of month')
                                			)
                                ) as o on product.id = o.product_id
                                group by product.name
                                "));
            PrintResult(Demo("Demo 5: Select from product table:", @"
                                select od.'m' as 'период', p.name as 'продукт', printf('%.2f',od.sa) as 'сумма', printf('%.0f', od.'d'*100) as 'доля'
                                from 
	                                (
		                                select o.'m', o.'p', max(o.'a') as sa, (o.'d')*1.0/(d.'dd')  as 'd'
		                                FROM (
				                                SELECT strftime('%Y-%m',dt) as 'm', product_id as 'p', sum(amount) as 'a', count(product_id) as 'd'
				                                FROM 'order'
				                                group by strftime('%Y-%m',dt), product_id
			                                 ) as o , 
			                                 (
			 	                                SELECT strftime('%Y-%m',dt) as 'm', count(product_id) as 'dd'
			 	                                FROM 'order'
			 	                                GROUP BY strftime('%Y-%m', dt)
			                                 ) as d
			                                 group by o.'m'
	                                ) AS od 
                                JOIN 
                                product p 
                                ON od.'p' = p.id
                                order by od.'m'"));

        }

        private static DataTable Demo(string name, string query) { 
            Console.WriteLine(name);
            return Query.ExecuteQuery(query);
            
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
            FileInfo f;
            try
            {
                f = new FileInfo(path);
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
                    if(s == "") break;
                    try
                    {
                        orderList.Add(s.ToLower().Trim(), i);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Columns name exception " + e.Message);
                    }
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
                {
                    Console.WriteLine("Line " + lineNum + ": Miss qunatity of values");
                    return null;
                }
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
                catch (FormatException e)
                {
                    Console.WriteLine("Line " + lineNum + ": " + e.Message);
                }
                finally
                {
                    lineNum++;
                }
            }
            fstr.Close();
            f.Delete();
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

        public static void PrintResult(DataTable result)
        {
            if (result == null) Console.WriteLine("None");
            else
            {
                foreach (DataColumn column in result.Columns)
                {
                    Console.Write(column.ColumnName + "  ");
                }
                Console.WriteLine();
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
}
