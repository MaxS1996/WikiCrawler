using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Data.SQLite;
using System.Collections.Concurrent;
using System.Threading;
using System.Net;

namespace WikipediaSearcher
{
    class Wikipedia
    {
        HashSet<string> todo = new HashSet<string>();

        ConcurrentDictionary<string, string> nodes = new ConcurrentDictionary<string, string>(Environment.ProcessorCount*2, 857);
        ConcurrentDictionary<Tuple<string, string>, bool> edges = new ConcurrentDictionary<Tuple<string, string>, bool>(Environment.ProcessorCount * 2, 857);

        HashSet<string> SavedNodes = new HashSet<string>();
        SQLiteConnection m_dbConnection;

        public Wikipedia(Uri start)
        {
            Article startArticle = new Article(start);
            if (!System.IO.File.Exists("WikiDB.sqlite"))
            {
                SQLiteConnection.CreateFile("WikiDB.sqlite");
                Console.WriteLine("Just entered to create Sync DB");

                //Creating Tables
                m_dbConnection = new SQLiteConnection("Data Source=WikiDB.sqlite;Version=3;");
                m_dbConnection.Open();

                string sql1 = "CREATE TABLE nodes(name VARCHAR(120), link VARCHAR(120))";
                string sql2 = "CREATE TABLE edges(strt VARCHAR(120), dst VARCHAR(120))";

                SQLiteCommand command1 = new SQLiteCommand(sql1, m_dbConnection);
                SQLiteCommand command2 = new SQLiteCommand(sql2, m_dbConnection);

                command1.ExecuteNonQuery();
                command2.ExecuteNonQuery();
            }
            else
            {
                m_dbConnection = new SQLiteConnection("Data Source=WikiDB.sqlite;Version=3;");
                m_dbConnection.Open();
                Console.WriteLine("Existing DB found!");
                LoadExisting();
                if(nodes.Values.Count != 0)
                {
                    Uri lastUri = new Uri(nodes.Values.Last<string>());
                    startArticle = new Article(lastUri);
                }
                else
                {
                    startArticle = new Article(start);
                }
                
            }

            Parallel.ForEach<string>(startArticle.Links, (currentLink) => { todo.Add(currentLink); });
            WorkOn(startArticle);
            Index();
            
            while (todo.Count != 0)
            {
                while ((nodes.Keys.Count != 0) && (edges.Count != 0))
                {
                    Console.ForegroundColor = ConsoleColor.White;
                    Parallel.Invoke(() => Index(), () => WriteNodes2DB(), () => WriteEdges2DB());
                }
                while (nodes.Count != 0 && edges.Count != 0)
                {
                    Console.ForegroundColor = ConsoleColor.Green;
                    Parallel.Invoke(() => WriteNodes2DB(), ()=>WriteEdges2DB());
                }
                while (nodes.Count != 0)
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    WriteNodes2DB();
                }
                while (edges.Count != 0)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    WriteEdges2DB();
                }
            }

        }

        public void Index()
        {
            HashSet<string> Links = new HashSet<string>();
            Parallel.ForEach<string>(todo, (link) =>
                                            {

                                                if ((link != null)&&(!nodes.ContainsKey(link)))
                                                {
                                                    try
                                                    {
                                                        Article article = new Article(new Uri(link));
                                                        WorkOn(article);
                                                        HashSet<string> newLinks = article.Links;
                                                        newLinks.Except(nodes.Keys);
                                                        Links.UnionWith(newLinks);
                                                    }
                                                    catch (Exception e)
                                                    {
                                                        Console.WriteLine(e.ToString());
                                                        System.Threading.Thread.Sleep(300000);
                                                        Article article = new Article(new Uri(link));
                                                        WorkOn(article);
                                                        HashSet<string> newLinks = article.Links;
                                                        newLinks.Except(nodes.Keys);
                                                        Links.UnionWith(newLinks);
                                                    }
                                                }
                                            });
            todo = Links;
        }

        public void LoadExisting()
        {
            Console.WriteLine("Loading saved Nodes!");
            string sql = "select * from nodes order by link desc";
            SQLiteCommand command = new SQLiteCommand(sql, m_dbConnection);
            SQLiteDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                Console.WriteLine("Node: " + reader["name"].ToString());
                nodes.GetOrAdd(reader["name"].ToString(), reader["link"].ToString());
            }
            Console.WriteLine("Loading existing Nodes completed");
        }

        public void WriteNodes2DB()
        {
            foreach(string name in this.nodes.Keys)
            {
                string link = "";
                bool worked = nodes.TryGetValue(name, out link);
                if (worked&&(!SavedNodes.Contains<string>(name)))
                {
                    string sql = "insert into nodes (name, link) values ('" + name + "','" + link + "')";
                    SQLiteCommand command = new SQLiteCommand(sql, m_dbConnection);
                    command.ExecuteNonQuery();
                    Console.WriteLine(sql);
                    SavedNodes.Add(name);
                }
                else
                {
                    Console.WriteLine("Access to "+link+" did not work");
                }
                
            }
        }

        private void WriteEdges2DB()
        {
            foreach (Tuple<string, string> edge in edges.Keys)
            {
                string sql = "insert into edges (strt, dst) values ('" + edge.Item1 + "','" + edge.Item2 + "')";
                SQLiteCommand command = new SQLiteCommand(sql, m_dbConnection);
                command.ExecuteNonQuery();

                edges.AddOrUpdate(edge, true, (key, oldValue) => true);
                edges.TryRemove(edge, out bool done);
                Console.WriteLine(sql);
            }
        }


        private void WorkOn(Article article)
        {
            nodes.GetOrAdd(article.Title, article.Address);
            Parallel.ForEach<string>(article.Links, (currentLink) => {
                edges.GetOrAdd(new Tuple<string, string>(article.Title, Article.ExtractTitle(currentLink)), false);
            });
            Console.WriteLine("["+(nodes.Keys.Count-1) +"] links: "+article.Links.Count+ "  in : "+article.Title);
        }

        public class Article
        {
            private String title = "";
            private Uri URL = null;

            private bool visited = false;
            private bool explored = false;

            private HashSet<String> links = new HashSet<string>();

            public Article(Uri address)
            {
                
                Regex link = new Regex("https://de.wikipedia.org/wiki/(.*?)");
                if (!link.IsMatch(address.ToString()))
                {
                    throw new ArgumentException();
                }
                this.URL = address;
                this.title = ExtractTitle(address);
                bool correct = Download().Result;
            }

            public static String ExtractTitle(Uri address)
            {
                string help = address.ToString();
                return ExtractTitle(help);
            }

            public static String ExtractTitle(string address)
            {
                string help = address;
                help = help.Replace("https://de.wikipedia.org/wiki/", "");
                help = help.Replace("http://de.wikipedia.org/wiki/", "");

                return help;
            }

            public async Task<bool> Download()
            {
                if (this.visited)
                {
                    return false;
                }

                HttpClient client = new HttpClient();
                HttpResponseMessage response = await client.GetAsync(this.URL);
                this.visited = true;
                if (response.StatusCode == HttpStatusCode.NotFound)
                {
                    return false;
                }

                string help = await response.Content.ReadAsStringAsync();
                help = System.Net.WebUtility.HtmlDecode(help);
                help = help.Replace("\n", "");
                Regex text = new Regex("<div class=\"mw-parser-output\">(.*?)<ol class=\"references\">");
                help = text.Match(help).Value;
                
                ExtractLinks(help);
                return true;
            }

            private void ExtractLinks(string text)
            {
                //<a href="/wiki/Elektrisches_Bauelement" title="Elektrisches Bauelement">
                if (!this.visited)
                {
                    return;
                }

                Regex link = new Regex("href=\"/wiki/(.*?)\"");
                Regex prefix = new Regex("href=\"");
                Regex suffix = new Regex("\"(.*)");
                Regex subarticle = new Regex("#(.*)");
                MatchCollection linkMatches = link.Matches(text);
                foreach(Match match in linkMatches)
                {
                    String help = prefix.Replace(match.Value, "https://de.wikipedia.org");
                    help = suffix.Replace(help, "");
                    help = subarticle.Replace(help, "");
                    if ((!help.Contains("Datei:"))&&(!help.Contains("Wikipedia:"))&&(!help.Equals(null)) && (!help.Contains("Spezial:")))
                    {
                        this.links.Add(help);
                    }
                }
            }

            public String Title { get => title; }
            public HashSet<String> Links { get => links; }
            public HashSet<String> LinkedArticles
            {
                get {
                    HashSet<String> linked = new HashSet<string>();
                    foreach (string link in links) {
                        linked.Add(Article.ExtractTitle(link));
                    }
                    return linked;
                }
            }
            public string Address { get => URL.ToString(); }
            public bool Visited { get => visited; }
            public bool Explored { get => explored; set { this.explored = value; } }
        }
    }
}
