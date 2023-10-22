using System.Globalization;
using Grpc.Core;

namespace LeaseManager
{
    internal class Program
    {
        public async static void Main(string[] args)
        {
            // <run> <clusterId> <id> <url> <port> <lms> <tms> <time_slot_duration> <start_time> <failure_suspicions> <debug?>

            foreach (string arg in args)
            {
                Console.WriteLine(arg);
            }

            if (args.Length < 9 || args.Length > 10)
            {
                Console.WriteLine("Wrong number of arguments");
                return;
            }

            bool debug = false;
            if (args.Length == 10 && args[9] == "debug")
                debug = true;

            LeaseManager leaseManager = new LeaseManager(int.Parse(args[0]), args[1], args[2], int.Parse(args[6]), LeaseManager.parseFailureSuspicions(args[8]), debug);

            Server server = new Server
            {
                Services = { LeaseManagerService.BindService(new LeaseManagerServiceImpl(leaseManager)) },
                Ports = { new ServerPort("localhost", int.Parse(args[3]), ServerCredentials.Insecure) }
            };
            server.Start();

            Thread.Sleep(2000); // wait for servers to start

            leaseManager.setLeaseManagerNodes(args[4]);
            leaseManager.setTmClusterNodes(args[5]);

            DateTime startTime = DateTime.ParseExact(args[7], "HH:mm:ss", CultureInfo.InvariantCulture);
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