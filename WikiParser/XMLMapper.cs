using System;
using System.IO;
using System.Xml;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using System.Diagnostics;

namespace WikiParser
{
    class XMLMapper
    {
        static int TotalAdded = 0;
        static int PreviousTotalAdded = 0;
        static long TotalBytes = 0;
        static long TotalProcessedBytes = 0;

        public static void Map(string[] file_list, Action<XmlReader, NpgsqlConnection> map_function)
        {
            Stopwatch st = new Stopwatch();
            st.Start();

            var input_tasks = new List<Task>();
            foreach (string filename in file_list) input_tasks.Add(Task.Run(() => Process(filename, map_function)));

            while (!Task.WaitAll(input_tasks.ToArray(), 1000))
            {
                var new_inserts = TotalAdded - PreviousTotalAdded;
                Console.Write("{0}% {1} operations a second\r", ((float)TotalProcessedBytes / TotalBytes) * 100, new_inserts);
                PreviousTotalAdded = TotalAdded;
            }
            st.Stop();
            Console.WriteLine("Executed {0} in {1}", map_function.Method.Name, st.Elapsed);

        }

        public static void ExecuteNonQuery(NpgsqlConnection conn, string command)
        {
            var query = new NpgsqlCommand(command, conn);
            query.ExecuteNonQuery();
        }


        public static void Process(string filename, Action<XmlReader, NpgsqlConnection> map_function)
        {
            Console.WriteLine("Processing file {0} with function {1}", filename, map_function.Method.Name);

            int local_counter = 0;

            var connection = PostgresSchema.GetConnection();
            connection.Open();
            ExecuteNonQuery(connection, "BEGIN TRANSACTION");
            using (FileStream file = new FileStream(filename, FileMode.Open))
            {
                Interlocked.Add(ref TotalBytes, file.Length);
                using (var reader = XmlReader.Create(file))
                {
                    while (reader.ReadToFollowing("page"))
                    {
                        var start_position = file.Position;

                        map_function(reader, connection);

                        Interlocked.Increment(ref TotalAdded);
                        Interlocked.Add(ref TotalProcessedBytes, file.Position - start_position);
                        local_counter++;
                        if (local_counter == 200)
                        {
                            ExecuteNonQuery(connection, "COMMIT");
                            local_counter = 0;
                        }
                    }
                }
            }
            ExecuteNonQuery(connection, "COMMIT");
            connection.Close();
        }
    }
}
