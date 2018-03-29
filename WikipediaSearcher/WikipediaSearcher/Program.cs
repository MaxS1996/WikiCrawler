using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WikipediaSearcher
{
    class Program
    {
        static void Main(string[] args)
        {
            Wiki wiki = new Wiki(new Uri("https://de.wikipedia.org/wiki/Berlin"));
            Console.ReadKey();
        }
    }
}
