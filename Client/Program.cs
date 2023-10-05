using System.Runtime.CompilerServices;

namespace Client
{
    internal class Program
    {
        public static void Main(string[] args)
        {
            if (args.Length != 2)
            {
                Console.Error.WriteLine("wrong arguments!");
                return;
            }

            string[] script = File.ReadAllLines(args[1]);

            Client client = new Client(args[0]);

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