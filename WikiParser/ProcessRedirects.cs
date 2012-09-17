using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using Npgsql;
using NpgsqlTypes;

namespace WikiParser
{
    class ProcessRedirects
    {
        public static void HandleRedirects(XmlReader reader, NpgsqlConnection connection)
        {
            var redirect_to = reader.GetAttribute("redirect");
            if (string.IsNullOrEmpty(redirect_to)) return;
            var title = reader.GetAttribute("title");

            var fetch_row_command = new NpgsqlCommand(@"with redirect_page as (SELECT id,title,redirect FROM pages WHERE title = :title)
 select * from redirect_page union all 
 select page.id,page.title,page.redirect from pages page 
 join redirect_page on page.title = redirect_page.redirect", connection);
            fetch_row_command.Parameters.AddWithValue("title", title);
            var query_reader = fetch_row_command.ExecuteReader();
            if (!query_reader.HasRows) return;
            query_reader.Read();
            int redirect_id = query_reader.GetInt32(0);
            query_reader.Read();
            int real_page_id = query_reader.GetInt32(0);
            query_reader.Close();

            var update = string.Format(@"UPDATE pages SET links = (links - ARRAY[{0}])  || ARRAY[{1}] WHERE links @> ARRAY[{0}];",
                redirect_id, redirect_id);

            var command = new NpgsqlCommand(update, connection);
            command.ExecuteScalar();
        }
    }
}
