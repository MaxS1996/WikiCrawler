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
    class Wiki
    {
        /// <summary>
        /// Found links, and their state
        /// 
        /// string => the url of the link
        /// bool => url explored?
        /// </summary>
        ConcurrentDictionary<string, bool> FoundLinks = new ConcurrentDictionary<string, bool>();

        /// <summary>
        /// Nodes which are going to be written to the DB
        /// </summary>
        ConcurrentDictionary<string, HashSet<string>> FinishedNodes= new ConcurrentDictionary<string, HashSet<string>>();


        SQLiteConnection m_dbConnection;

        public Wiki(Uri startUri)
        {
            Article startArticle = new Article(startUri);
            Console.WriteLine("logical cores: "+System.Environment.ProcessorCount);
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
                if (FoundLinks.Count != 0)
                {
                    Uri lastUri = new Uri(FoundLinks.Keys.Last<string>());
                    startArticle = new Article(lastUri);
                }
            }
            while (FinishedNodes.Count < 50)
            {
                WorkOn(startArticle);
                Random rand = new Random();
                if(FoundLinks.Count > 220)
                {
                    startArticle = new Article(new Uri(FoundLinks.Keys.ElementAt(rand.Next(FoundLinks.Keys.Count - 200, FoundLinks.Keys.Count))));
                }
                else
                {
                    startArticle = new Article(new Uri(FoundLinks.Keys.ElementAt(rand.Next(FoundLinks.Keys.Count))));
                }
                
            }
            while (!FoundLinks.Keys.Equals(FinishedNodes.Keys))
            {
                Parallel.Invoke(() => Index(), () => Write2DB());
            }

            

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
                FoundLinks.GetOrAdd(reader["link"].ToString(), true);
            }
            Console.WriteLine("Loading existing Nodes completed");
        }

        public void Index()
        {
            //ParallelOptions options = new ParallelOptions { MaxDegreeOfParallelism = 60* Environment.ProcessorCount };
            Parallel.ForEach<KeyValuePair<string, bool>>(FoundLinks/*, options*/, (link) =>
            {
                if ((link.Value == false))
                {
                    try
                    {
                        Article article = new Article(new Uri(link.Key));
                        WorkOn(article);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("COULD NOT DOWNLOAD THE ARTICLE "+ link.Key);
                        Thread.Sleep(300000);
                        Article article = new Article(new Uri(link.Key));
                        WorkOn(article);
                    }
                }
            });

        }

        private void WorkOn(Article article)
        {
            FinishedNodes.GetOrAdd(article.Address, article.Links);
            Parallel.ForEach<string>(article.Links, (currentLink) => { FoundLinks.GetOrAdd(currentLink, false); });
            FoundLinks.TryUpdate(article.Address, true, false);
            Console.WriteLine("[" + FinishedNodes.Keys.Count + "] "+"links: " + article.Links.Count + "  in : " + article.Title);
        }

        private void Write2DB()
        {
            while (!FoundLinks.Keys.Equals(FinishedNodes.Keys))
            {
                if (!FinishedNodes.Values.All(value => value==null)/*FoundLinks.Values.Contains(false)*/)
                {
                    foreach (KeyValuePair<string, HashSet<string>> link in FinishedNodes)
                    {
                        string name = Article.ExtractTitle(link.Key);
                        name = name.Replace("'", "''");
                        string sql = "BEGIN TRANSACTION;";
                        sql = sql + " insert into nodes (name, link) values ('" + name + "','" + link.Key.Replace("'","''") + "');";

                        HashSet<string> edges = link.Value;
                        if (edges == null)
                        {
                            continue;
                        }
                        foreach (string edge in edges)
                        {
                            sql = sql + "insert into edges (strt, dst) values ('" + name + "','" + Article.ExtractTitle(edge).Replace("'", "''") + "');";
                        }

                        sql = sql + "COMMIT;";
                        SQLiteCommand command = new SQLiteCommand(m_dbConnection);
                        command.CommandType = System.Data.CommandType.Text;
                        command.CommandText = sql;
                        Console.WriteLine(name + " commited to DB with " + edges.Count + " outgoing edges");
                        command.ExecuteNonQuery();
                        FinishedNodes.TryUpdate(link.Key, null, link.Value);
                    }
                }
                else
                {
                    Article startArticle;
                    Random rand = new Random();
                    if (FoundLinks.Count > 220)
                    {
                        startArticle = new Article(new Uri(FoundLinks.Keys.ElementAt(rand.Next(FoundLinks.Keys.Count - 200, FoundLinks.Keys.Count))));
                    }
                    else
                    {
                        startArticle = new Article(new Uri(FoundLinks.Keys.ElementAt(rand.Next(FoundLinks.Keys.Count))));
                    }
                    WorkOn(startArticle);                    
                }
            }
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
                foreach (Match match in linkMatches)
                {
                    String help = prefix.Replace(match.Value, "https://de.wikipedia.org");
                    help = suffix.Replace(help, "");
                    help = subarticle.Replace(help, "");
                    if ((!help.Contains("Datei:")) && (!help.Contains("Wikipedia:")) && (!help.Equals(null)) && (!help.Contains("Spezial:")))
                    {
                        this.links.Add(help);
                    }
                }
            }

            public String Title { get => title; }
            public HashSet<String> Links { get => links; }
            public HashSet<String> LinkedArticles
            {
                get
                {
                    HashSet<String> linked = new HashSet<string>();
                    foreach (string link in links)
                    {
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
