
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Data.Common;
using System.Reflection;
using System.Runtime.CompilerServices;
using Google.Protobuf.Collections;
using Grpc.Net.Client;

namespace LeaseManager
{
    public class InstanceState
    {
        private int readTS = -1;
        private int writeTS = -1;
        private List<Lease> value = new List<Lease>();
        private bool decided = false;
        private bool no_op = false;

        public InstanceState() { }
        public int getRTS() { return readTS; }
        public int getWTS() { return writeTS; }
        public List<Lease> getValue() { return value; }
        public bool isDecided() { return decided; }
        public bool isNo_op() { return no_op; }

        public void setRTS(int readTS) { this.readTS = readTS; }
        public void setWTS(int writeTS) { this.writeTS = writeTS; }
        public void setValue(List<Lease> value) { this.value = value; }
        public void setDecided(bool decided) { this.decided = decided; }
        public void setNo_op(bool no_op) { this.no_op = no_op; }

        public bool containsLease(Lease lease)
        {
            if (value == null)
                return false;
            foreach (Lease l in value)
            {
                if (l.Equals(lease))
                    return true;
            }
            return false;
        }
    }

    public class PaxosNode
    {
        private int id;
        private Dictionary<int, LeaseManagerService.LeaseManagerServiceClient> paxosClusterNodes = new Dictionary<int, LeaseManagerService.LeaseManagerServiceClient>();
        private Dictionary<int, List<int>> failureSuspicions = new Dictionary<int, List<int>>(); // each index is a timeSlot and each entry is a list of suspected nodes
        private int currentInstance = 0;
        private int lastKnownLeader = 0;
        private int previousPriorityLeader;

        private bool leader = false;

        private int roundId = 0;
        private int mostRecentReadTS = 0;

        private ConcurrentDictionary<int, InstanceState> instancesStates = new ConcurrentDictionary<int, InstanceState>();
        private int lastNotifiedInstance = 0;

        private List<Lease> leasesQueue = new List<Lease>(); // leases that have not been handled yet, key is the lease and value is the number of times it has been requested


        public PaxosNode(int id)
        {
            this.id = id;

            if (id == 0)
            {
                previousPriorityLeader = paxosClusterNodes.Count - 1;
                leader = true;
            }
            else
                previousPriorityLeader = id - 1;
        }

        public int getLastNotifiedInstance()
        {
            return lastNotifiedInstance;
        }

        public void setLastNotifiedInstance(int instanceId)
        {
            lastNotifiedInstance = instanceId;
        }

        public void setClusterNodes(Dictionary<int, GrpcChannel> channels)
        {
            foreach (KeyValuePair<int, GrpcChannel> pair in channels)
            {
                paxosClusterNodes[pair.Key] = new LeaseManagerService.LeaseManagerServiceClient(pair.Value);
            }
        }

        public void setFailureSuspicions(Dictionary<int, List<int>> failureSuspicions)
        {
            this.failureSuspicions = failureSuspicions;
        }

        public void runPaxosInstance()
        {
            currentInstance++;
            instancesStates[currentInstance] = new InstanceState();
            if (leader)
            {
                broadcastAccept(currentInstance);
            }
            else
            {
                if (isLeaderCandidate())
                {
                    broadcastPrepare();
                }
            }

        }

        public async void broadcastPrepare()
        {
            setRoundId(roundId + 1);
            int ballotId = calcBallotId();

            List<int> unresolvedInstances = new List<int>();
            foreach (KeyValuePair<int, InstanceState> pair in instancesStates)
            {
                if (!pair.Value.isDecided())
                    unresolvedInstances.Add(pair.Key);
            }

            PrepareRequest request = new PrepareRequest
            {
                Id = this.id,
                BallotId = ballotId,
                UnresolvedInstances = { unresolvedInstances },
            };

            List<Task<PrepareResponse>> responseTasks = new List<Task<PrepareResponse>>();
            foreach (KeyValuePair<int, LeaseManagerService.LeaseManagerServiceClient> pair in paxosClusterNodes)
            {
                Task<PrepareResponse> response = pair.Value.PrepareAsync(request).ResponseAsync;
                responseTasks.Add(response);
            }

            await PrepareWaitForMajority(responseTasks);
        }
        private async Task PrepareWaitForMajority(List<Task<PrepareResponse>> responseTasks)
        {
            int nacksCount = 0;
            int promisesCount = 0;

            while (true)
            {
                Task<PrepareResponse> finishedTask = await Task.WhenAny(responseTasks);
                responseTasks.Remove(finishedTask);
                PrepareResponse response = await finishedTask;

                if (!response.Ok)
                {
                    nacksCount++;
                    setRoundId(Math.Max(roundId, (response.MostRecentReadTS - response.MostRecentReadTS % getClusterSize()) / getClusterSize()));
                    if (nacksCount > paxosClusterNodes.Count / 2)
                    {
                        setLeader(false);
                        break;
                    }
                }
                else
                {
                    promisesCount++;
                    foreach (KeyValuePair<int, InstanceStateMessage> kvp in response.InstancesStates)
                    {
                        // if the instance state is more recent than the one stored, update it
                        if (kvp.Value.WriteTS > instancesStates[kvp.Key].getWTS())
                        {
                            instancesStates[kvp.Key].setWTS(kvp.Value.WriteTS);
                            instancesStates[kvp.Key].setValue(LeasesListMessageToLeasesList(kvp.Value.Value));
                        }
                    }

                    if (promisesCount > paxosClusterNodes.Count / 2)
                    {
                        // here the node sending the prepares is ready to send accept msgs, it will do this for each instance that has not been decided yet

                        leader = true;
                        setLastKnownLeader(id);
                        foreach (KeyValuePair<int, InstanceState> kvp in instancesStates)
                        {
                            if (!kvp.Value.isDecided())
                            {
                                if (kvp.Value.getValue() == null && kvp.Key < currentInstance)
                                    kvp.Value.setNo_op(true);
                                broadcastAccept(kvp.Key);
                            }
                        }
                        break;
                    }
                }
            }
        }

        public async void broadcastAccept(int instance)
        {
            AcceptRequest request = new AcceptRequest
            {
                Id = this.id,
                InstanceId = instance,
                BallotId = calcBallotId(),
            };

            if (instancesStates[instance].isNo_op())
                request.Value = null;
            else
            {
                List<Lease> valueToPropose = instancesStates[instance].getValue();
                valueToPropose ??= calcValueToPropose();
                request.Value = leasesListToLeasesListMessage(valueToPropose);
            }

            List<Task<AcceptResponse>> responseTasks = new List<Task<AcceptResponse>>();

            foreach (KeyValuePair<int, LeaseManagerService.LeaseManagerServiceClient> pair in paxosClusterNodes)
            {
                Task<AcceptResponse> response = pair.Value.AcceptAsync(request).ResponseAsync;
                responseTasks.Add(response);
            }

            await AcceptWaitForMajority(responseTasks);
        }
        private async Task AcceptWaitForMajority(List<Task<AcceptResponse>> responseTasks)
        {
            int nacksCount = 0;
            int acceptsCount = 0;

            while (true)
            {
                Task<AcceptResponse> finishedTask = await Task.WhenAny(responseTasks);
                responseTasks.Remove(finishedTask);
                AcceptResponse response = await finishedTask;

                if (!response.Ok)
                {
                    nacksCount++;
                    setRoundId(Math.Max(roundId, (response.MostRecentReadTS - response.MostRecentReadTS % getClusterSize()) / getClusterSize()));

                    // here the node sending these accepts should stop being a leader because it has seen that a node with a higher ballotId (has priority) is playing the proposer role
                    if (nacksCount > paxosClusterNodes.Count / 2)
                    {
                        leader = false;
                        break;
                    }
                }
                else
                {
                    acceptsCount++;
                    if (acceptsCount > paxosClusterNodes.Count / 2)
                    {
                        broadcastDecided(response.InstanceId);
                    }
                }
            }
        }

        public void broadcastDecided(int instance)
        {
            DecidedRequest request = new DecidedRequest
            {
                Id = this.id,
                InstanceId = instance,
                BallotId = calcBallotId(),
                Value = leasesListToLeasesListMessage(instancesStates[instance].value),
            };

            foreach (KeyValuePair<int, LeaseManagerService.LeaseManagerServiceClient> pair in paxosClusterNodes)
            {
                pair.Value.Decided(request);
            }
        }

        private List<Lease> calcValueToPropose()
        {
            List<Lease> valueToPropose = new List<Lease>();
            bool found = false;
            foreach (Lease lease in leasesQueue)
            {
                foreach (KeyValuePair<int, InstanceState> pair in instancesStates)
                {
                    if (pair.Value.containsLease(lease))
                    {
                        found = true;
                        break;
                    }
                }
                if (!found)
                    valueToPropose.Add(lease);
                else
                    leasesQueue.Remove(lease);
                found = false;
            }
            return valueToPropose;
        }

        private bool isLeaderCandidate()
        {
            return lastKnownLeader == previousPriorityLeader && failureSuspicions[currentInstance].Contains(previousPriorityLeader);
        }

        public bool isLeader()
        {
            return leader;
        }

        public void setLeader(bool leader)
        {
            this.leader = leader;
        }

        public void setLastKnownLeader(int leaderId)
        {
            if (leaderId != lastKnownLeader)
                lastKnownLeader = leaderId;
        }

        public int getMostRecentReadTS()
        {
            return mostRecentReadTS;
        }

        public void setMostRecentReadTS(int ts)
        {
            mostRecentReadTS = ts;
        }

        public void setRoundId(int roundId)
        {
            this.roundId = roundId;
        }

        private int calcBallotId()
        {
            return roundId * paxosClusterNodes.Count + id;
        }

        public void addLeaseToQueue(Lease lease)
        {
            leasesQueue.Add(lease);
        }

        public int getClusterSize()
        {
            return paxosClusterNodes.Count;
        }

        public static LeaseMessageLM leaseToLeaseMessage(Lease lease)
        {
            LeaseMessageLM lease_Grpc = new LeaseMessageLM
            {
                ClientId = lease.TmId,
            };
            foreach (string key in lease.Keys)
                lease_Grpc.DataKeys.Add(key);
            return lease_Grpc;
        }

        public static LeasesListMessageLM leasesListToLeasesListMessage(List<Lease> leases)
        {
            LeasesListMessageLM leasesList_Grpc = new LeasesListMessageLM();
            foreach (Lease lease in leases)
                leasesList_Grpc.Leases.Add(leaseToLeaseMessage(lease));
            return leasesList_Grpc;
        }

        public static Lease LeaseMessageToLease(LeaseMessageLM lease_Grpc)
        {
            Lease lease = new Lease(lease_Grpc.ClientId, lease_Grpc.DataKeys.ToList());
            return lease;
        }

        public static List<Lease> LeasesListMessageToLeasesList(LeasesListMessageLM value)
        {
            List<Lease> leases = new List<Lease>();
            foreach (LeaseMessageLM lease in value.Leases)
                leases.Add(LeaseMessageToLease(lease));
            return leases;
        }

        public InstanceState getInstanceState(int instanceId)
        {
            return instancesStates[instanceId];
        }

        public static InstanceStateMessage instanceStateToInstanceStateMessage(InstanceState instanceState)
        {
            InstanceStateMessage instanceStateMessage = new InstanceStateMessage
            {
                ReadTS = instanceState.getRTS(),
                WriteTS = instanceState.getWTS(),
                Value = leasesListToLeasesListMessage(instanceState.getValue()),
                Decided = instanceState.isDecided(),
            };
            return instanceStateMessage;
        }
    }
}