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
        private int lastHandledInstance = 0;

        private Dictionary<string, int> lmsIds_lmsClusterIds = new Dictionary<string, int>();
        private Dictionary<int, GrpcChannel> lmClusterIds_channels = new Dictionary<int, GrpcChannel>();
        private Dictionary<int, GrpcChannel> tmClusterIds_channels = new Dictionary<int, GrpcChannel>();
        private Dictionary<int, (string, TransactionManagerService.TransactionManagerServiceClient)> ids_tmsServices = new Dictionary<int, (string, TransactionManagerService.TransactionManagerServiceClient)>();

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

        public void Logger(string message)
        {
            if (this.debug) Console.WriteLine($"(TimeStamp: {DateTime.UtcNow}): [ LM {this.id} ]\t" + message + '\n');
        }

        public void setLeaseManagerNodes(string lms)
        {
            // Channel to self
            GrpcChannel channel;
            channel = GrpcChannel.ForAddress(url);
            lmClusterIds_channels[clusterId] = channel;

            string[] keyValuePairs = lms.Split('!', StringSplitOptions.RemoveEmptyEntries);

            foreach (string pair in keyValuePairs)
            {
                string[] parts = pair.Split('-');
                int n = int.Parse(parts[0]);
                string id = parts[1];
                string url = parts[2];

                channel = GrpcChannel.ForAddress(url); // sets up channels to lm nodes
                lmClusterIds_channels[n] = channel;
                lmsIds_lmsClusterIds[id] = n;
            }
            setPaxosCluster(); // sets up paxos cluster nodes
            this.Logger($"set lease managers");
        }

        private void setPaxosCluster()
        {
            paxosNode.setClusterNodes(lmClusterIds_channels);
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
                this.tmClusterIds_channels[n] = channel;
                this.ids_tmsServices[n] = (id, client);
            }
            this.Logger($"set transaction managers, cluster with {count} nodes");
        }

        internal PaxosNode getPaxosNode()
        {
            return paxosNode;
        }

        public void registerLease(string tmId, List<string> dataKeys)
        {
            Lease lease = new Lease(tmId, dataKeys);
            receivedLeases.Add(lease);
            paxosNode.addLeaseToQueue(lease);
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
                string? line;
                while ((line = reader.ReadLine()) != null)
                {
                    if (line.StartsWith("F"))
                    {
                        string[] parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                        int timeSlot = int.Parse(parts[1]);
                        int lmsStatesStartIndex = 2 + ids_tmsServices.Count;

                        if (parts[lmsStatesStartIndex + clusterId] == "C")
                            crashTimeSlot = timeSlot;

                        for (int i = lmsStatesStartIndex + lmClusterIds_channels.Count; i < parts.Length; i++)
                        {
                            string[] sus = parts[i].Trim('(', ')').Split(',');
                            Console.WriteLine($"instance {timeSlot}: sus[0]: {sus[0]}, sus[1]: {sus[1]}");
                            if (sus[0] == id)
                            {
                                if (suspicions.ContainsKey(timeSlot))
                                {
                                    suspicions[timeSlot].Add(lmsIds_lmsClusterIds[sus[1]]);
                                }
                                else
                                {
                                    suspicions[timeSlot] = new List<int>
                                        {
                                            lmsIds_lmsClusterIds[sus[1]]
                                        };
                                }
                            }
                        }
                    }
                }
            }
            paxosNode.setFailureSuspicions(suspicions);

            foreach (KeyValuePair<int, List<int>> entry in suspicions)
            {
                this.Logger($"suspicions at time slot {entry.Key}: {string.Join(", ", entry.Value)}");
            }
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

        public void notifyClients(int lastTimeSlotRun)
        {
            while (lastHandledInstance <= lastTimeSlotRun)
            {
                int instanceToPropagate = lastHandledInstance + 1;

                if (paxosNode.getInstancesStates().ContainsKey(instanceToPropagate))
                {
                    if (paxosNode.getInstanceState(instanceToPropagate).isNoOp()) // FIXME:
                    {
                        Logger($"Instance {instanceToPropagate} resulted in a no-op, not propagating this.");
                        lastHandledInstance++;
                        continue;
                    }

                    if (paxosNode.getInstanceState(instanceToPropagate).isDecided())
                    {
                        Logger($"propagating instance {instanceToPropagate} result.");
                        InstanceResultRequest request = new InstanceResultRequest()
                        {
                            LmId = clusterId,
                            InstanceId = instanceToPropagate,
                        };
                        foreach (Lease l in paxosNode.getInstanceState(instanceToPropagate).getValue())
                            request.Result.Add(leaseToLeaseMessageTM(l));

                        foreach (KeyValuePair<int, (string, TransactionManagerService.TransactionManagerServiceClient)> entry in ids_tmsServices)
                            entry.Value.Item2.InstanceResultAsync(request);

                        lastHandledInstance++;
                    }
                    else
                    {
                        this.Logger($"instance {instanceToPropagate} not decided yet.");
                        break;
                    }
                }
                else
                    break;
            }
        }

        public static string LeasesListToString(List<Lease> leases)
        {
            string result = "";
            foreach (Lease lease in leases)
                result += lease.ToString() + ";";
            return result;
        }

        public void closeChannels()
        {
            foreach (KeyValuePair<int, GrpcChannel> entry in lmClusterIds_channels)
                entry.Value.ShutdownAsync();
            foreach (KeyValuePair<int, GrpcChannel> entry in tmClusterIds_channels)
                entry.Value.ShutdownAsync();
        }

        public void startService()
        {
            int currentTimeSlot = 1;

            Timer timer = new Timer(state =>
            {
                if (currentTimeSlot > 1)
                    notifyClients(currentTimeSlot - 1);

                if (currentTimeSlot == crashTimeSlot)
                {
                    this.Logger($"crashing at time slot {currentTimeSlot}");
                    closeChannels();
                    Environment.Exit(0);
                }

                if (currentTimeSlot <= timeSlots)
                {
                    this.Logger($"executing paxos instance {currentTimeSlot}");
                    paxosNode.runPaxosInstance(currentTimeSlot);
                }

                if (lastHandledInstance == timeSlots)
                {
                    closeChannels();
                    Environment.Exit(0);
                }
                else
                    currentTimeSlot++;

            }, null, 0, timeSlotDuration);
        }
    }
}
