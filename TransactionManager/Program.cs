using Grpc.Core;
using System.Globalization;

namespace TransactionManager
{
    class Program
    {
        private async static Task MainAsync(string[] args)
        {
            // <clusterId> <id> <url> <lms> <tms> <time_slots> <start_time> <time_slot_duration> <config_file> <debug?>

            if (args.Length < 9 || args.Length > 10)
            {
                Console.Error.WriteLine("[TransactionManager] Wrong number of arguments!");
                return;
            }

            bool debug = false;
            if (args.Length == 10 && args[9] == "debug")
                debug = true;

            TransactionManager transactionManager = new TransactionManager(int.Parse(args[0]), args[1], args[2], debug);

            var uri = new Uri(args[2]);
            string host = uri.Host;
            int port = uri.Port;

            ServerPort serverPort = new ServerPort("localhost", port, ServerCredentials.Insecure);

            Server server = new Server
            {
                Services = { TransactionManagerService.BindService(new TransactionManagerServiceImpl(transactionManager)) },
                Ports = { serverPort }
            };

            server.Start();

            Thread.Sleep(1000); // wait for servers to start

            transactionManager.configureExecution(int.Parse(args[5]), int.Parse(args[7]));
            transactionManager.configureStateAndSuspicions(args[8]);
            transactionManager.SetTmClusterNodes(args[4]);
            transactionManager.SetLmClusterNodes(args[3]);

            DateTime startTime = DateTime.ParseExact(args[6], "HH:mm:ss", CultureInfo.InvariantCulture);
            DateTime currentTime = DateTime.Now;
            if (startTime > currentTime)
            {
                TimeSpan delay = startTime - currentTime;
                await Task.Delay(delay);
            }

            int epoch = 0;
            Timer timer = new Timer(async state =>
            {
                epoch++;
                if (epoch == int.Parse(args[7])) await server.KillAsync();
            }, null, 0, int.Parse(args[5]));
        }

        public static void Main(string[] args)
        {
            MainAsync(args).GetAwaiter().GetResult();
        }
    }
}