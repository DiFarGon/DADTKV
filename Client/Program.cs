using System.Globalization;
using System.Runtime.CompilerServices;

namespace Client
{
    internal class Program
    {
        public async static Task MainAsync(string[] args)
        {
            // <id> <tms> <tm_id> <startTime> <client_script> <debug?>

            if (args.Length < 5 || args.Length > 6)
            {
                Console.Error.WriteLine("[Client] Wrong number of arguments!");
                return;
            }

            bool debug = false;
            if (args.Length == 6 && args[5] == "debug")
            {
                debug = true;
            }

            Client client = new Client(args[0], args[2], debug);

            DateTime startTime = DateTime.ParseExact(args[3], "HH:mm:ss", CultureInfo.InvariantCulture);
            DateTime currentTime = DateTime.Now;
            if (startTime > currentTime)
            {
                TimeSpan delay = startTime - currentTime;
                await Task.Delay(delay);
            }

            Thread.Sleep(2000);

            client.setTmClusterNodes(args[1]);

            string[] script = File.ReadAllLines(args[4]);
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
            client.closeChannel();
        }

        public static void Main(string[] args)
        {
            MainAsync(args).GetAwaiter().GetResult();
        }
    }
}