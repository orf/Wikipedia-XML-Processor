using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace WikiParser
{
    class InputNamesIntoDatabase
    {
        static SemaphoreSlim datlock = new SemaphoreSlim(initialCount:2); // Be nice and let postgresql have some CPU

        public static void InputNames(string[] input_files)
        {
            var input_tasks = new List<Task>();
            foreach (string filename in input_files) input_tasks.Add(Task.Run(() => ProcessXML(filename)));
            Task.WaitAll(input_tasks.ToArray());
        }

        public static string Quote(string str, char quotechar = '\'')
        {
            if (string.IsNullOrEmpty(str)) return string.Empty;
            return string.Format("\"{0}\"",str.Replace(@"\", @"\\").Replace(quotechar.ToString(), string.Format(@"\{0}",quotechar)));
        }

        private static void ProcessXML(string filename)
        {
            datlock.Wait();
            Console.WriteLine("{0}: Inputting {1}", Thread.CurrentThread.ManagedThreadId, filename);

            var sql = string.Format(@"COPY pages (title, redirect) FROM '{0}' WITH (FORMAT 'csv', DELIMITER '|', ESCAPE '\')", filename);

            var connection = PostgresSchema.GetConnection();
            connection.Open();
            var query = new NpgsqlCommand(sql, connection);
            int rows = query.ExecuteNonQuery();
            Console.WriteLine("{0}: Added {1} rows", Thread.CurrentThread.ManagedThreadId, rows);
            connection.Close();
            datlock.Release();
        }
    }
}
