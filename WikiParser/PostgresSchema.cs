using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using Npgsql;

namespace WikiParser
{
    class PostgresSchema
    {
        static string CONNECTION_STRING = "Server=localhost;User Id=wikilink;Password=password;Database=wikilinks;commandtimeout=0;MinPoolSize=10";

        static string EXTENSION_SQL = "CREATE EXTENSION IF NOT EXISTS intarray;";
        static string CREATE_TABLE_SQL = @"CREATE TABLE pages (
                id SERIAL,
                title varchar(255) NOT NULL,
                redirect varchar(255),
                links integer[],
                CONSTRAINT pkey PRIMARY KEY (id))";

        static string TITLE_INDEX = @"CREATE UNIQUE INDEX title_index
                ON pages
                USING btree (title)";

        static string LINK_INDEX = @"CREATE INDEX links_index
                ON pages 
                USING gin (links gin__int_ops)
                WHERE links IS NOT null";

        public static string LINK_ID_QUERY = @"SELECT DISTINCT id FROM pages WHERE title IN ({0})
                                                      AND redirect IS NULL
                                                      UNION SELECT id FROM pages WHERE title IN (
                                                        SELECT redirect FROM pages WHERE title IN ({0}) AND redirect IS NULL
                                                      )";

        public static void SetupSchema()
        {
            ExecuteSQL(EXTENSION_SQL);
            ExecuteSQL("DROP TABLE IF EXISTS pages"); // take out the trash
            ExecuteSQL(CREATE_TABLE_SQL);
        }

        public static void CreateTitleIndex()
        {
            ExecuteSQL("DROP INDEX IF EXISTS title_index");
            CreateIndex("Titles", TITLE_INDEX);
        }

        public static void CreateLinkIndex()
        {
            ExecuteSQL("DROP INDEX IF EXISTS links_index");
            CreateIndex("Links", LINK_INDEX);
        }

        public static void CreateIndex(string title, string sql)
        {
            Console.WriteLine("Creating index {0}", title);
            var stopwatch = new Stopwatch();
            stopwatch.Start();
            ExecuteSQL(sql);
            stopwatch.Stop();
            Console.WriteLine("Created index in {0}", stopwatch.Elapsed);
            
        }

        public static NpgsqlConnection GetConnection()
        {
            return new NpgsqlConnection(CONNECTION_STRING);
           
        }

        public static void ExecuteSQL(string sql)
        {
            using (var conn = GetConnection())
            {
                conn.Open();
                var command = new NpgsqlCommand(sql, conn);
                command.ExecuteNonQuery();
                conn.Close();
            }
        }
    }
}
