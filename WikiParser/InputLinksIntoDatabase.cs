using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading.Tasks;
using System.Xml;
using System.Diagnostics;
using System.Threading;
using Npgsql;
using NpgsqlTypes;

namespace WikiParser
{
    class InputLinksIntoDatabase
    {
        public static void ProcessLinks(XmlReader reader, NpgsqlConnection connection)
        {
            var page_title = reader.GetAttribute("title");
            List<string> titles;
            XmlReader inner;
            inner = reader.ReadSubtree();
            titles = new List<string>();
            while (inner.ReadToFollowing("link")) titles.Add(inner.ReadElementContentAsString());

            if (titles.Count() == 0) return;

            // Now we have a list of titles construct a subquery to get the ID's
            // I could use this query, which looks nicer, but EXPLAIN says it costs a little bit more.
            // SELECT id FROM pages WHERE title IN ('Apple','ElementalAllotropes')
            // AND redirect IS NULL UNION SELECT p2.id FROM pages AS p1 
            // JOIN pages AS p2 ON p1.title IN ('Apple','ElementalAllotropes') AND p2.title = p1.redirect AND p2.redirect IS NULL;
            var select_command_text = string.Format(PostgresSchema.LINK_ID_QUERY,
                                string.Join(",", (from val in Enumerable.Range(0, titles.Count)
                                                select string.Format(":title_{0}", val))));
 

            // Update our links to be the array of ID's using the subquery above
            var command = new NpgsqlCommand(string.Format("UPDATE pages SET links = ARRAY({0}) WHERE title = :title", select_command_text), connection);
            foreach (var val in Enumerable.Range(0, titles.Count)) command.Parameters.AddWithValue("title_"+val, titles[val]);
            command.Parameters.Add(new NpgsqlParameter("title", page_title));
            command.ExecuteNonQuery();
        }
    }
}
