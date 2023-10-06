using System.Runtime.CompilerServices;

namespace Client
{
    internal class Program
    {
        // <id> <url> <tm_url> <tms> <startTime> <debug?>

        public static void Main(string[] args)
        {
            if (args.Length < 5 || args.Length > 6)
            {
                Console.Error.WriteLine("[Client] Wrong number of arguments!");
                return;
            }

            string[] script = File.ReadAllLines(args[1]);

            bool debug = false;
            if (args.Length == 6 && args[5] == "debug")
            {
                debug = true;
            }

            Client client = new Client(args[0], args[2], debug);

            Thread.Sleep(5000); // testing purposes

            foreach (string line in script)
            {
                switch (line[0])
                {
                    case '#':
                        continue;
                    case 'T':
                        client.handleT(line);
                        break;
                    case 'W':
                        client.handleW(line);
                        break;
                    case 'S':
                        client.handleS();
                        break;
                }
            }

        }
    }
}