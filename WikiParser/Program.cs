using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using Npgsql;
using System.Web.Security;

namespace WikiParser
{
    class Program
    {
        static string XML_DIRECTORY = @"D:\WikiDump\";
        static string OUTPUT_DIRECTORY = @"C:\output\";
        

        static void Main(string[] args)
        {
            var watch = new Stopwatch();
            watch.Start();
            
            if (!args.Contains("--skip-parse"))
            {
                // Setup: Clear the OUTPUT_DIRECTORY
                foreach (string file in Directory.EnumerateFiles(OUTPUT_DIRECTORY)) File.Delete(file);
                // Step #1: Run a regular expression over every page and process the XML document
                // into a smaller "bitesize" version that we can easily input into the database
                foreach (string file_path in Directory.GetFiles(XML_DIRECTORY, "*.xml"))
                {
                    var parser = new ParsePageText();
                    parser.ParseFile(file_path, OUTPUT_DIRECTORY);
                }
                Console.WriteLine("Parsed files in {0}", watch.Elapsed);
                watch.Stop();
            }

            if (!args.Contains("--skip-import"))
            {
                // Step #2: Create our database schema
                PostgresSchema.SetupSchema();
                // Step #3: Read all the *titles* from all our XML files and input them into the database
                InputNamesIntoDatabase.InputNames(Directory.GetFiles(OUTPUT_DIRECTORY, "*.csv"));
                PostgresSchema.CreateTitleIndex();
            }

            if (!args.Contains("--skip-links"))
            {
                // Step #4: Insert all the links into the database
                XMLMapper.Map(Directory.GetFiles(OUTPUT_DIRECTORY, "*.xml"), InputLinksIntoDatabase.ProcessLinks);
                // Step #5: Create the link index
                Console.WriteLine("Creating the Link index, this may take a *while*");
                PostgresSchema.CreateLinkIndex();
                PostgresSchema.ExecuteSQL("DELETE FROM pages WHERE redirect IS NOT NULL");
            }
        }
    }
}