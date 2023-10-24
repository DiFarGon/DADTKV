using System.Globalization;
using Grpc.Core;

namespace LeaseManager
{
    class Program
    {
        public async static Task MainAsync(string[] args)
        {
            // <clusterId> <id> <url> <lms> <tms> <time_slots> <start_time> <time_slot_duration> <config_file> <debug?>

            if (args.Length < 9 || args.Length > 10)
            {
                Console.WriteLine("Wrong number of arguments");
                return;
            }

            bool debug = false;
            if (args.Length == 10 && args[9] == "debug")
                debug = true;

            LeaseManager leaseManager = new LeaseManager(int.Parse(args[0]), args[1], args[2], debug);

            var uri = new Uri(args[2]);
            string host = uri.Host;
            int port = uri.Port;

            ServerPort serverPort = new ServerPort("localhost", port, ServerCredentials.Insecure);

            Server server = new Server
            {
                Services = { LeaseManagerService.BindService(new LeaseManagerServiceImpl(leaseManager)) },
                Ports = { serverPort }
            };
            server.Start();

            Thread.Sleep(1000); // wait for servers to start

            leaseManager.configureExecution(int.Parse(args[5]), int.Parse(args[7]));
            leaseManager.setLeaseManagerNodes(args[3]);
            leaseManager.setTmClusterNodes(args[4]);
            leaseManager.configureStateAndSuspicions(args[8]);


            // DateTime startTime = DateTime.ParseExact(args[6], "HH:mm:ss", CultureInfo.InvariantCulture);
            // DateTime currentTime = DateTime.Now;
            // if (startTime > currentTime)
            // {
            //     TimeSpan delay = startTime - currentTime;
            //     await Task.Delay(delay);
            // }
            // leaseManager.startService();
        }

        public static void Main(string[] args)
        {
            MainAsync(args).GetAwaiter().GetResult();
        }
    }
}