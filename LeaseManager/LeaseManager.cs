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
        private int clusterId;
        private string url;
        private bool debug;
        private PaxosNode paxosNode;

        private int timeSlots;
        private int timeSlotDuration;
        private int crashTimeSlot = -1;

        private Dictionary<int, GrpcChannel> ids_channels = new Dictionary<int, GrpcChannel>();

        private Dictionary<int, (string, TransactionManagerService.TransactionManagerServiceClient)> ids_tmsServices =
            new Dictionary<int, (string, TransactionManagerService.TransactionManagerServiceClient)>();

        private List<Lease> receivedLeases = new List<Lease>();

        public LeaseManager(int clusterId, string id, string url, bool debugMode)
        {
            this.id = id;
            this.clusterId = clusterId;
            this.url = url;
            this.debug = debugMode;

            paxosNode = new PaxosNode(clusterId);

            this.Logger("created");
        }

        public void configureExecution(int timeSlots, int timeSlotDuration)
        {
            this.timeSlotDuration = timeSlotDuration;
            this.timeSlots = timeSlots;
        }

        public void configureStateAndSuspicions(string configFile)
        {
            Dictionary<int, List<int>> suspicions = new Dictionary<int, List<int>>();
            using StreamReader reader = new StreamReader(configFile);
            {
                string line;
                {
                    while ((line = reader.ReadLine()) != null)
                    {
                        if (line.TrimStart().StartsWith("F"))
                        {
                            string[] l = line.Substring(2).TrimStart().Split(' ');
                            int timeSlot = int.Parse(l[0]);
                            if (l[ids_tmsServices.Count + clusterId] == "C")
                                crashTimeSlot = timeSlot;

                            for (int i = 1 + ids_tmsServices.Count + ids_channels.Count; i < l.Length; i++)
                            {
                                string[] sus = l[i].Trim('(', ')').Split(',');
                                if (sus[0] == id)
                                {
                                    if (suspicions.ContainsKey(timeSlot))
                                        suspicions[timeSlot].Add(int.Parse(sus[1]));
                                    else
                                    {
                                        suspicions[timeSlot] = new List<int>
                                        {
                                            int.Parse(sus[1])
                                        };
                                    }
                                }
                            }
                        }
                    }
                }
            }
            paxosNode.setFailureSuspicions(suspicions);
        }

        public void Logger(string message)
        {
            if (this.debug) Console.WriteLine($"(TimeStamp: {DateTime.UtcNow}): [ LM {this.id} ]\t" + message + '\n');
        }

        public void setLeaseManagerNodes(string lms)
        {
            // Channel to self
            GrpcChannel channel;
            channel = GrpcChannel.ForAddress(url);
            ids_channels[clusterId] = channel;

            string[] keyValuePairs = lms.Split('!', StringSplitOptions.RemoveEmptyEntries);

            foreach (string pair in keyValuePairs)
            {
                string[] parts = pair.Split('-');
                int n = int.Parse(parts[0]);
                string id = parts[1];
                string url = parts[2];

                channel = GrpcChannel.ForAddress(url); // sets up channels to lm nodes
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

        public static Dictionary<int, List<int>> parseFailureSuspicions(string failureSuspicions)
        {
            Dictionary<int, List<int>> suspicions = new Dictionary<int, List<int>>();

            string[] susps = failureSuspicions.Split('!', StringSplitOptions.RemoveEmptyEntries);

            foreach (string susp in susps)
            {
                string[] parts = susp.Split(':');
                if (!suspicions.ContainsKey(int.Parse(parts[0])))
                    suspicions[int.Parse(parts[0])] = new List<int>();
                foreach (string s in parts[1].Split(','))
                    suspicions[int.Parse(parts[0])].Add(int.Parse(s));
            }
            return suspicions;
        }

        public void registerLease(string tmId, List<string> dataKeys)
        {
            Lease lease = new Lease(tmId, dataKeys);
            receivedLeases.Add(lease);
            paxosNode.addLeaseToQueue(lease);
        }

        public static LeaseMessageTM leaseToLeaseMessageTM(Lease lease)
        {
            LeaseMessageTM leaseMessage = new LeaseMessageTM
            {
                ClientId = lease.TmId,
            };
            foreach (string key in lease.Keys)
                leaseMessage.DataKeys.Add(key);
            return leaseMessage;
        }

        public static LeasesListMessageTM leasesListToLeasesListMessageTM(List<Lease> leases)
        {
            LeasesListMessageTM leasesListMessage = new LeasesListMessageTM();
            foreach (Lease lease in leases)
                leasesListMessage.Leases.Add(leaseToLeaseMessageTM(lease));
            return leasesListMessage;
        }

        public void notifyClients()
        {
            while (true)
            {
                int instanceToNotify = paxosNode.getLastNotifiedInstance() + 1;
                if (paxosNode.getInstanceState(instanceToNotify).decided)
                {
                    InstanceResultRequest request = new InstanceResultRequest()
                    {
                        LmId = clusterId,
                        InstanceId = instanceToNotify,
                    };

                    if (!paxosNode.getInstanceState(instanceToNotify).no_op)
                        request.Result = leasesListToLeasesListMessageTM(paxosNode.getInstanceState(instanceToNotify).value);
                    else
                        request.Result = null;

                    foreach (KeyValuePair<int, (string, TransactionManagerService.TransactionManagerServiceClient)> entry in ids_tmsServices)
                    {
                        entry.Value.Item2.InstanceResult(request);
                    }
                    paxosNode.setLastNotifiedInstance(instanceToNotify);
                }
                else
                    break;

            }
        }

        public async void startService(DateTime startTime)
        {
            DateTime currentTime = DateTime.Now;
            if (startTime > currentTime)
            {
                TimeSpan delay = startTime - currentTime;
                await Task.Delay(delay);
            }

            int currentTimeSlot = 0;
            bool executionCompleted = false;
            Timer timer = new Timer(state =>
            {
                if (!executionCompleted)
                {
                    currentTimeSlot++;

                    paxosNode.runPaxosInstance();

                    if (currentTimeSlot >= timeSlots)
                        executionCompleted = true;

                    if (currentTimeSlot == crashTimeSlot)
                        Environment.Exit(0);

                    notifyClients();
                }
            }, null, 0, timeSlotDuration);
        }
    }
}
