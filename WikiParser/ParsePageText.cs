using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Xml;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Threading;
using System.Collections.Concurrent;
using System.Globalization;
using System.Diagnostics;

namespace WikiParser
{
    class ParsePageText
    {
        int VolumeRead = 0;

        Regex LinkExtractor = new Regex(@"\[\[(.*?)(?:\|[^]]*)?\]\]", RegexOptions.Compiled);
        List<string> initial_ignored = new List<string>{ "Media", "Special", "Talk", "User","Wiktionary",
                                                          "User talk", "Wikipedia", "Wikipedia talk",
                                                          "File", "File talk", "MediaWiki", "MediaWiki talk",
                                                          "Template", "Template talk", "Help", "Help talk",
                                                          "Category", "Category talk", "Portal", "Portal talk",
                                                          "Book", "Book talk", "File", "Image", "Simple"};

        HashSet<string> ignore_titles = new HashSet<string>();

        private double ConvertToMB(long bytes)
        {
            return (bytes / 1024f) / 1024f;
        }
        

        public void ParseFile(string file_path, string output_folder)
        {
            foreach (var cc in (from ci in CultureInfo.GetCultures(CultureTypes.AllCultures) select ci.TwoLetterISOLanguageName)) ignore_titles.Add(cc.ToUpper());
            foreach (var cc in (from ci in CultureInfo.GetCultures(CultureTypes.AllCultures) select ci.ThreeLetterISOLanguageName)) ignore_titles.Add(cc.ToUpper());

            foreach (var ignored in initial_ignored) ignore_titles.Add(ignored.ToUpper());

            Console.WriteLine("Parsing file {0}", file_path);

            using (FileStream file = new FileStream(file_path, FileMode.Open))
            {
                var reader = XmlReader.Create(file);
                var input_queue = new BlockingCollection<Page>(50); // Hold 500 pages in the queue before blocking.

                var tasks = new List<Task>();
                foreach (int id in Enumerable.Range(0, System.Environment.ProcessorCount)) tasks.Add(Task.Run(() => HandlePages(id, input_queue, output_folder)));

                var timer = new Stopwatch();
                timer.Start();
                long last_bytes = 0;

                while (reader.ReadToFollowing("page"))
                {
                    XmlReader InnerPage = reader.ReadSubtree();
                    Page page = ParsePage(InnerPage);
                    if (ignore_titles.Contains(page.Title.Split(':')[0].ToUpper())) continue;
                    input_queue.Add(page);
                    VolumeRead++;

                    if (timer.Elapsed >= TimeSpan.FromSeconds(1))
                    {
                        timer.Restart();
                        Console.Write("{0}, {1} MB/s\r", (float)file.Position / file.Length * 100, ConvertToMB(file.Position - last_bytes));

                        last_bytes = file.Position;
                    }
                }
                Console.WriteLine("\nFinished Adding");
                input_queue.CompleteAdding();
                Task.WaitAll(tasks.ToArray());
            }
        }

        private void HandlePages(int id, BlockingCollection<Page> input, string output_folder)
        {
            string output_file_base = output_folder + System.IO.Path.GetRandomFileName(); 
            string output_file = output_file_base  + ".xml";
            string output_file_csv = output_file_base + ".csv";
            
            Console.WriteLine("{0}: Outputting to {1} and {2}", id, output_file, output_file_csv);
            var settings = new XmlWriterSettings();
            settings.Indent = true;

            using (var writer = XmlWriter.Create(output_file, settings))
            {
                using (var csv_writer = new StreamWriter(output_file_csv))
                {
                    writer.WriteStartElement("pages");
                    foreach (var page in input.GetConsumingEnumerable())
                    {
                        writer.WriteStartElement("page");
                        writer.WriteAttributeString("title", page.Title);
                        writer.WriteAttributeString("redirect", page.RedirectTo);

                        var matches = LinkExtractor.Matches(page.Text);
                        var results = (from string title in
                                           (from Match m in matches select m.Groups[1].Value.Split('#')[0])
                                       where title.Length < 255 && title.Length > 0
                                       where !ignore_titles.Contains(title.Split(':')[0].ToUpper())
                                       select title.Substring(0, 1).ToUpper() + title.Substring(1)).Distinct();

                        if (page.RedirectTo == null)
                        {
                            writer.WriteStartElement("links");
                            foreach (string link in results) writer.WriteElementString("link", link);
                            writer.WriteEndElement();
                        }
                        writer.WriteEndElement();
                        //Interlocked.Increment(ref VolumeParsed);
                        csv_writer.WriteLine(string.Format("{0}|{1}", InputNamesIntoDatabase.Quote(page.Title, '"'),
                                                                              InputNamesIntoDatabase.Quote(page.RedirectTo,'"')));
                    }
                    writer.WriteEndElement();
                    writer.Flush();
                }
            }
            return;
        }

        private Page ParsePage(XmlReader PageElement)
        {
            Page page = new Page();
            int GotCount = 0;
            while (PageElement.Read())
            {
                switch (PageElement.Name)
                {
                    case "title":
                        page.Title = PageElement.ReadElementString();
                        GotCount++;
                        break;
                    case "redirect":
                        page.RedirectTo = PageElement.GetAttribute("title");
                        GotCount++;
                        break;
                    case "text":
                        page.Text = PageElement.ReadElementString();
                        GotCount++;
                        break;
                }
                if (GotCount == 3) break;
            }
            return page;
        }
    }
}
