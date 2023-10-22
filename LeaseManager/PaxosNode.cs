
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
        public int readTS = -1; // TODO: maybe don't need to store this, i think the mostRecentReadTS from PaxosNode is enough
        public int writeTS = -1;
        public List<Lease>? value = null;
        public bool decided = false;
        public bool no_op = false;

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
        private List<List<bool>> failureSuspicions;
        private int currentInstance = 0;
        private int lastKnownLeader = 0;
        private bool leader = false;

        private int roundId = 0;
        private int ballotId; // combination of nodes id and round id (e.g. for node 1 and round 2, ballotId = 2 * clusterSize + 1 = 3)
        private int mostRecentReadTS = 0;
        private ConcurrentDictionary<int, InstanceState> instancesStates = new ConcurrentDictionary<int, InstanceState>();

        private List<Lease> leasesQueue = new List<Lease>(); // leases that have not been handled yet, key is the lease and value is the number of times it has been requested


        public PaxosNode(int id, List<List<bool>> failureSuspicions)
        {
            this.id = id;
            this.ballotId = id;

            this.failureSuspicions = failureSuspicions;

            if (id == 0)
            {
                leader = true;
            }

        }

        public void setClusterNodes(Dictionary<int, GrpcChannel> channels)
        {
            foreach (KeyValuePair<int, GrpcChannel> pair in channels)
            {
                paxosClusterNodes[pair.Key] = new LeaseManagerService.LeaseManagerServiceClient(pair.Value);
            }
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

        private bool isLeaderCandidate() // FIXME: this might change depending on the failure suspicions format
        {
            int previousPriorityLeader; // TODO: maybe make this a field
            if (id == 0)
                previousPriorityLeader = paxosClusterNodes.Count - 1;
            else
                previousPriorityLeader = id - 1;

            return lastKnownLeader == previousPriorityLeader && !failureSuspicions[currentInstance][previousPriorityLeader];
        }

        public bool isLeader()
        {
            return leader;
        }

        public void setLeader(bool leader)
        {
            this.leader = leader;
        }

        public async void broadcastPrepare()
        {
            incrementBallotId();

            List<int> unresolvedInstances = new List<int>();
            foreach (KeyValuePair<int, InstanceState> pair in instancesStates)
            {
                if (!pair.Value.decided)
                {
                    unresolvedInstances.Add(pair.Key);
                }
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
                    setRoundId(Math.Max(roundId, (response.MostRecentReadTS - response.Id) / paxosClusterNodes.Count));
                    if (nacksCount > paxosClusterNodes.Count / 2)
                    {
                        break;
                    }
                }
                else
                {
                    promisesCount++;
                    foreach (var kvp in response.InstancesStates)
                    {
                        // if the instance state is more recent than the one stored, update it
                        if (kvp.Value.WriteTS > instancesStates[kvp.Key].writeTS)
                        {
                            instancesStates[kvp.Key].writeTS = kvp.Value.WriteTS;
                            instancesStates[kvp.Key].value = grpcLeasesListToLeasesList(kvp.Value.Value);
                        }
                    }

                    if (promisesCount > paxosClusterNodes.Count / 2)
                    {
                        leader = true;
                        setLastKnownLeader(id);
                        // here the node sending the prepares is ready to send accept msgs, it will do this for each instance that has not been decided yet
                        foreach (KeyValuePair<int, InstanceState> pair in instancesStates)
                        {
                            if (!pair.Value.decided)
                            {
                                if (pair.Value.value == null && pair.Key < currentInstance)
                                {
                                    pair.Value.no_op = true;
                                }
                                broadcastAccept(pair.Key);
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
                BallotId = ballotId,
            };

            if (!instancesStates[instance].no_op)
            {
                request.Value = null;
            }
            else
            {
                List<Lease>? valueToPropose = instancesStates[instance].value;

                if (valueToPropose == null)
                {
                    valueToPropose = calcValueToPropose();
                }
                request.Value = leasesListToGrpcLeasesList(valueToPropose);
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
                    setRoundId(Math.Max(roundId, (response.MostRecentReadTS - response.Id) / paxosClusterNodes.Count));

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

                    // FIXME: here the node sending the accepts should broadcast decided msgs (to all learners?) for this instance
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
                BallotId = ballotId,
                Value = leasesListToGrpcLeasesList(instancesStates[instance].value),
            };

            foreach (KeyValuePair<int, LeaseManagerService.LeaseManagerServiceClient> pair in paxosClusterNodes)
            {
                DecidedResponse response = pair.Value.Decided(request);
            }
            // FIXME: should the lease manager also send to the clients?
        }


        private List<Lease> calcValueToPropose()
        {
            List<Lease> valueToPropose = new List<Lease>();
            bool found = false;
            foreach (Lease lease in leasesQueue)
            {
                foreach (KeyValuePair<int, InstanceState> pair in instancesStates)
                {
                    if (pair.Value.writeTS != -1 && pair.Value.containsLease(lease))
                    {
                        found = true;
                        break;
                    }
                }
                if (!found)
                    valueToPropose.Add(lease);
            }
            return valueToPropose;
        }

        public void setLastKnownLeader(int leaderId)
        {
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

        private void incrementBallotId()
        {
            roundId++;
            ballotId = roundId * paxosClusterNodes.Count + id;
        }

        public void addLeaseToQueue(Lease lease)
        {
            leasesQueue.Add(lease);
        }

        public int getClusterSize()
        {
            return paxosClusterNodes.Count;
        }

        public static Lease_grpc leaseToGrpcLease(Lease lease)
        {
            Lease_grpc lease_Grpc = new Lease_grpc
            {
                ClientId = lease.TmId,
            };
            foreach (string key in lease.Keys)
                lease_Grpc.DataKeys.Add(key);
            return lease_Grpc;
        }

        public static LeasesList_grpc leasesListToGrpcLeasesList(List<Lease> leases)
        {
            LeasesList_grpc leasesList_Grpc = new LeasesList_grpc();
            foreach (Lease lease in leases)
                leasesList_Grpc.Leases.Add(leaseToGrpcLease(lease));
            return leasesList_Grpc;
        }

        public static Lease grpcLeaseToLease(Lease_grpc lease_Grpc)
        {
            Lease lease = new Lease(lease_Grpc.ClientId, lease_Grpc.DataKeys.ToList());
            return lease;
        }

        public static List<Lease> grpcLeasesListToLeasesList(LeasesList_grpc value)
        {
            List<Lease> leases = new List<Lease>();
            foreach (Lease_grpc lease_Grpc in value.Leases)
                leases.Add(grpcLeaseToLease(lease_Grpc));
            return leases;
        }

        public InstanceState getInstanceState(int instanceId)
        {
            return instancesStates[instanceId];
        }

        public static InstanceState_grpc instanceStateToGrpcInstanceState(InstanceState instanceState)
        {
            InstanceState_grpc instanceState_Grpc = new InstanceState_grpc
            {
                ReadTS = instanceState.readTS,
                WriteTS = instanceState.writeTS,
                Value = leasesListToGrpcLeasesList(instanceState.value),
                Decided = instanceState.decided,
            };
            return instanceState_Grpc;
        }

        public void setInstanceStateWriteTS(int instanceId, int wts)
        {
            instancesStates[instanceId].writeTS = wts;
        }

        public void setInstanceStateValue(int instanceId, List<Lease>? value)
        {
            if (value == null)
                instancesStates[instanceId].no_op = true;
            else
                instancesStates[instanceId].value = value;
        }

        public void setInstanceStateDecided(int instanceId)
        {
            instancesStates[instanceId].decided = true;
        }
    }
}