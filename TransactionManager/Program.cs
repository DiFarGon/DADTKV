using Grpc.Core;
using System;
using System.Runtime.CompilerServices;

namespace TransactionManager
{
    class Program
    {
        public static void Main(string[] args)
        {
            if (args.Length != 2)
            {
                Console.Error.WriteLine("wrong arguments!");
                return;
            }
            TransactionManager transactionManager = new TransactionManager(args[0], args[1]);

            var uri = new Uri(args[1]);
            string host = uri.Host;
            int port = uri.Port;

            ServerPort serverPort = new ServerPort("localhost", port, ServerCredentials.Insecure);

            Server server = new Server
            {
                Services = { ClientService.BindService(new TransactionManagerServiceImpl(transactionManager)) },
                Ports = { serverPort }
            };

            server.Start();

            while (true) ;
        }
    }
}