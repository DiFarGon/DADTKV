using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;
using Grpc.Net.Client;


namespace LeaseManager
{
    public class LeaseManager
    {
        private string id;
        private string url;
        private bool debug;
        private PaxosNode paxosNode;

        private int timeSlotDuration;

        private Dictionary<int, GrpcChannel> ids_channels = new Dictionary<int, GrpcChannel>(); // key is the node's id and value is (lm id, channel)

        private Dictionary<int, (string, LeaseManagerService.LeaseManagerServiceClient)> ids_lmsServices =
            new Dictionary<int, (string, LeaseManagerService.LeaseManagerServiceClient)>();

        private Dictionary<int, (string, TransactionManagerService.TransactionManagerServiceClient)> ids_tmsServices =
            new Dictionary<int, (string, TransactionManagerService.TransactionManagerServiceClient)>();

        private List<Lease> receivedLeases = new List<Lease>();

        public LeaseManager(int clusterId, string id, string url, int timeSlotDuration, List<List<bool>> failureSuspicions, bool debugMode)
        {
            this.id = id;
            this.url = url;
            this.debug = debugMode;

            this.timeSlotDuration = timeSlotDuration;

            GrpcChannel channel = GrpcChannel.ForAddress(url);
            ids_channels[clusterId] = channel;
            ids_lmsServices[clusterId] = (id, new LeaseManagerService.LeaseManagerServiceClient(channel)); // TODO: not sure if needed since node will only communicate with himself during paxos

            paxosNode = new PaxosNode(clusterId, failureSuspicions);

            this.Logger("created");
        }

        public void Logger(string message)
        {
            if (this.debug) Console.WriteLine($"(TimeStamp: {DateTime.UtcNow}): [ LM {this.id} ]\t" + message + '\n');
        }

        public void setLeaseManagerNodes(string lms)
        {
            string[] keyValuePairs = lms.Split('!', StringSplitOptions.RemoveEmptyEntries);

            foreach (string pair in keyValuePairs)
            {
                string[] parts = pair.Split('-');
                int n = int.Parse(parts[0]);
                string id = parts[1];
                string url = parts[2];

                GrpcChannel channel = GrpcChannel.ForAddress(url); // sets up channels to lm nodes
                ids_lmsServices[n] = (id, new LeaseManagerService.LeaseManagerServiceClient(channel)); // sets up lm nodes
                ids_channels[n] = channel;
            }
            setPaxosCluster(); // sets up paxos cluster nodes
            this.Logger($"set lease managers");
        }

        private void setPaxosCluster()
        {
            paxosNode.setClusterNodes(ids_channels);
        }

        public void setTmClusterNodes(string tms)
        {
            string[] keyValuePairs = tms.Split('!', StringSplitOptions.RemoveEmptyEntries);

            int count = 0;
            foreach (string pair in keyValuePairs)
            {
                count++;

                string[] parts = pair.Split('-');
                int n = int.Parse(parts[0]);
                string id = parts[1];
                string url = parts[2];

                GrpcChannel channel = GrpcChannel.ForAddress(url);
                TransactionManagerService.TransactionManagerServiceClient client = new TransactionManagerService.TransactionManagerServiceClient(channel);
                this.ids_tmsServices[n] = (id, client);
            }
            this.Logger($"set transaction managers, cluster with {count} nodes");
        }

        internal PaxosNode getPaxosNode()
        {
            return paxosNode;
        }

        // FIXME: maybe the type will not be List<List<bool>> but something else seen as the script will have (lm1, lm2) if lm1 suspects lm2
        public static List<List<bool>> parseFailureSuspicions(string failureSuspicions)
        {
            List<List<bool>> suspicions = new List<List<bool>>();
            // FIXME:
            return suspicions;
        }

        public void registerLease(string tmId, List<string> dataKeys)
        {
            Lease lease = new Lease(tmId, dataKeys);
            receivedLeases.Add(lease);
            paxosNode.addLeaseToQueue(lease);
        }

        public void startService()
        {
            Timer timer = new Timer(state => paxosNode.runPaxosInstance(), null, 0, timeSlotDuration);
        }
    }
}
