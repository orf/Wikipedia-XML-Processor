using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WikiParser
{
    class Page
    {
        public string Title;
        public string RedirectTo;
        public List<string> Links = new List<string>();
        public string Text;
    }
}
