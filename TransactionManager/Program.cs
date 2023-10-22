using Grpc.Core;
using System;
using System.Runtime.CompilerServices;

namespace TransactionManager
{
    class Program
    {
        public static void Main(string[] args)
        {
            // <clusterId> <id> <url> <lms> <tms> <debug?>

            if (args.Length < 5 || args.Length > 6)
            {
                Console.Error.WriteLine("[TransactionManager] Wrong number of arguments!");
                return;
            }

            bool debug = false;
            if (args.Length == 6 && args[5] == "debug")
                debug = true;

            TransactionManager transactionManager = new TransactionManager(int.Parse(args[0]), args[1], args[2], debug);

            var uri = new Uri(args[2]);
            string host = uri.Host;
            int port = uri.Port;

            ServerPort serverPort = new ServerPort("localhost", port, ServerCredentials.Insecure);

            Server server = new Server
            {
                Services = { ClientService.BindService(new TransactionManagerServiceImpl(transactionManager)) },
                Ports = { serverPort }
            };

            server.Start();

            Thread.Sleep(2000); // wait for servers to start

            transactionManager.setTmClusterNodes(args[4]);
            transactionManager.setLmClusterNodes(args[3]);

            DateTime beggining = DateTime.Now;

            while (true)
            {
                if (DateTime.Now - beggining > TimeSpan.FromSeconds(30))
                    break;
            };
        }
    }
}