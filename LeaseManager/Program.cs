using System.Globalization;
using Grpc.Core;

namespace LeaseManager
{
    internal class Program
    {
        public async static void Main(string[] args)
        {
            // <clusterId> <id> <url> <lms> <tms> <time_slots> <start_time> <time_slot_duration> <crash_time_slot> <failure_suspicions> <debug?>

            foreach (string arg in args)
            {
                Console.WriteLine(arg);
            }

            if (args.Length < 10 || args.Length > 11)
            {
                Console.WriteLine("Wrong number of arguments");
                return;
            }

            bool debug = false;
            if (args.Length == 11 && args[10] == "debug")
                debug = true;

            LeaseManager leaseManager = new LeaseManager(int.Parse(args[0]), args[1], args[2], debug);

            Server server = new Server
            {
                Services = { LeaseManagerService.BindService(new LeaseManagerServiceImpl(leaseManager)) },
                Ports = { new ServerPort("localhost", int.Parse(args[3]), ServerCredentials.Insecure) }
            };
            server.Start();

            Thread.Sleep(1000); // wait for servers to start

            leaseManager.configureExecution(int.Parse(args[5]), int.Parse(args[7]));
            leaseManager.configureStateAndSuspicions(args[8], args[9]);
            leaseManager.setLeaseManagerNodes(args[3]);
            leaseManager.setTmClusterNodes(args[4]);

            DateTime startTime = DateTime.ParseExact(args[6], "HH:mm:ss", CultureInfo.InvariantCulture);
            DateTime currentTime = DateTime.Now;
            if (startTime > currentTime)
            {
                TimeSpan delay = startTime - currentTime;
                await Task.Delay(delay);
            }
            leaseManager.startService();
        }
    }
}