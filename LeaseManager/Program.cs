using Grpc.Core;

namespace LeaseManager
{
    internal class Program
    {
        public static void Main(string[] args)
        {
            // <run> <clusterId> <id> <url> <port> <lms> <tms> <time_between_paxos_instances> <debug?>
            // TODO: check args? 

            if (args.Length < 7 || args.Length > 8)
            {
                Console.WriteLine("Wrong number of arguments");
                return;
            }

            bool debug = false;
            if (args.Length == 8 && args[7] == "debug")
                debug = true;

            LeaseManager leaseManager = new LeaseManager(int.Parse(args[0]), args[1], args[2], debug);

            Server server = new Server
            {
                Services = { LeaseManagerService.BindService(new LeaseManagerServiceImpl(leaseManager)) },
                Ports = { new ServerPort("localhost", int.Parse(args[3]), ServerCredentials.Insecure) }
            };
            server.Start();

            Thread.Sleep(2000); // wait for servers to start

            leaseManager.setPaxosClusterNodes(args[4]);
            leaseManager.setTmClusterNodes(args[5]);

            DateTime lastPaxosInstance = DateTime.Now;

            //  TODO: initiate paxos instance, call it taking into account the time between paxos instances and the timeouts suspecting the proposer
            while (true)
            {
                //  TODO: should i take into account here the 2sec sleep time?
                if (DateTime.Now - lastPaxosInstance > TimeSpan.FromSeconds(int.Parse(args[6])) && leaseManager.isIdle())
                {
                    // leaseManager.controlMsgs();
                    if (leaseManager.isProposer()) leaseManager.propose();
                }
                lastPaxosInstance = DateTime.Now;
            }

        }
    }
}