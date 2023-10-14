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
    public class LeaseManagerNew
    {
        private string id;
        private string url;
        private bool debug;
        private PaxosNode paxosNode;

        private Dictionary<int, GrpcChannel> ids_channels = new Dictionary<int, GrpcChannel>(); // key is the node's id and value is (lm id, channel)

        private Dictionary<int, (string, LeaseManagerService.LeaseManagerServiceClient)> ids_lmsServices =
            new Dictionary<int, (string, LeaseManagerService.LeaseManagerServiceClient)>();

        private Dictionary<int, (string, TransactionManagerService.TransactionManagerServiceClient)> ids_tmsServices =
            new Dictionary<int, (string, TransactionManagerService.TransactionManagerServiceClient)>();

        private Dictionary<Lease, int> unhandledLeases = new Dictionary<Lease, int>(); // leases that have not been handled yet, key is the lease and value is the number of times it has been requested
        private Dictionary<Lease, int> handledLeases = new Dictionary<Lease, int>();
        private List<Lease> receivedLeases = new List<Lease>();

        public LeaseManagerNew(int clusterId, string id, string url, string startTime, bool debugMode)
        {
            this.id = id;
            this.url = url;
            this.debug = debugMode;

            GrpcChannel channel = GrpcChannel.ForAddress(url);
            ids_channels[clusterId] = channel;
            ids_lmsServices[clusterId] = (id, new LeaseManagerService.LeaseManagerServiceClient(channel)); // TODO: not sure if needed since node will only communicate with himself during paxos

            paxosNode = new PaxosNode(clusterId);
            paxosNode.leasesUpdateRequest += handleLeasesUpdate;

            this.Logger("created");
        }

        private void handleLeasesUpdate(Dictionary<int, List<Lease>> consensusLog, List<int> processedInstances, bool updateValueToPropose)
        {
            // consume consensus log entries not yet processed to update handledLeases
            foreach (KeyValuePair<int, List<Lease>> pair in consensusLog)
            {
                if (!processedInstances.Contains(pair.Key))
                {
                    foreach (Lease lease in pair.Value)
                    {
                        handledLeases[lease]++;
                    }
                }
            }
            // if the paxos node needs a new value to propose, update it
            if (updateValueToPropose)
            {
                // goes through the leases that arrived 
                foreach (KeyValuePair<Lease, int> pair1 in unhandledLeases)
                {
                    // goes through the leases that have already been handled
                    foreach (KeyValuePair<Lease, int> pair2 in handledLeases)
                        if (pair1.Key.Equals(pair2.Key))
                        {
                            // take into account that the lease that arrived may have been requested more than once and may already have been handled
                            unhandledLeases[pair1.Key] -= pair2.Value;
                            // if it has been handled the same number of times it has been requested (or more), remove it from the unhandled leases
                            if (unhandledLeases[pair1.Key] <= 0)
                            {
                                unhandledLeases.Remove(pair1.Key);
                            }
                        }
                }
                // update the value to propose to the unhandled leases
                paxosNode.setValueToPropose(unhandledLeases.Keys.ToList());
            }
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

        public void obtainLeasesToGrant()
        {
            // paxosNode
        }
    }
}
