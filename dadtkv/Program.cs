using System;
using System.Diagnostics;
using System.Globalization;

namespace Dadtkv
{
    internal class Program
    {
        public static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                Console.Error.WriteLine("Not enough arguments!");
                return;
            }

            string[] script = File.ReadAllLines(args[0]);
            foreach (string line in script)
            {
                switch (line[0])
                {
                    case '#':
                        continue;
                    case 'P':
                        handleP(line);
                        break;
                    case 'S':
                        handleS(line);
                        break;
                    case 'D':
                        handleD(line);
                        break;
                    case 'T':
                        handleT(line);
                        break;
                    case 'F':
                        handleF(line);
                        break;
                }
            }
            return;
        }

        private static void handleP(string line)
        {
            string[] tokens = line.Split(' ');
            string id = tokens[1];
            string type = tokens[2];
            string url = tokens[3];

            switch (type)
            {
                case "T":
                    newTransactionManager(id, url);
                    break;
                case "L":
                    newLeaseManager(id, url);
                    break;
                case "C":
                    newClient(id, url);
                    break;
            }
        }

        private static void handleS(string line)
        {
            string slots = line.Split(' ')[1];

            Console.WriteLine("Test lasts {0} slots", slots);
        }

        private static void handleD(string line)
        {
            string time = line.Split(' ')[1];

            Console.WriteLine("Each slot lasts {0} miliseconds", time);
        }

        private static void handleT(string line)
        {
            string time = line.Split(' ')[1];

            Console.WriteLine("Test starts at {0}", time);
        }

        private static void handleF(string line)
        {
            Console.WriteLine("I don't really want to do this right now");
        }

        private static void newTransactionManager(string id, string url)
        {
            Console.WriteLine("Creating new transaction manager with id {0} and url {1}", id, url);
        }

        private static void newLeaseManager(string id, string url)
        {
            Console.WriteLine("Creating new lease manager with id {0} and url {1}", id, url);
        }

        private static void newClient(string id, string url)
        {
            Console.WriteLine("Creating new client with id {0} and url {1}", id, url);
        }
    }
}