using System.Runtime.CompilerServices;

namespace Client
{
    internal class Program
    {
        public static void Main(string[] args)
        {
            // <id> <tms> <tm_id> <startTime> <debug?>

            if (args.Length < 4 || args.Length > 5)
            {
                Console.Error.WriteLine("[Client] Wrong number of arguments!");
                return;
            }

            bool debug = false;
            if (args.Length == 5 && args[4] == "debug")
            {
                debug = true;
            }

            Client client = new Client(args[0], args[2], debug);

            Thread.Sleep(1000);

            client.setTmClusterNodes(args[1]);

            // string[] script = File.ReadAllLines(args[1]);

            // ///Reads script in a loop
            // while (true)
            // {
            //     foreach (string line in script)
            //     {
            //         switch (line[0])
            //         {
            //             case '#':
            //                 continue;
            //             case 'T':
            //                 client.handleT(line);
            //                 break;
            //             case 'W':
            //                 client.handleW(line);
            //                 break;
            //             case 'S':
            //                 client.handleS();
            //                 break;
            //         }
            //     }
            // }

            client.closeChannel();
        }
    }
}