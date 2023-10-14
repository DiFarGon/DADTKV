
using System.Data.Common;
using System.Runtime.CompilerServices;
using Grpc.Net.Client;

namespace LeaseManager
{
    public class PaxosNode
    {
        private int id;
        private Dictionary<int, PaxosNodeService.PaxosNodeServiceClient> paxosClusterNodes = new Dictionary<int, PaxosNodeService.PaxosNodeServiceClient>();
        private Dictionary<int, List<Lease>> consensusLog = new Dictionary<int, List<Lease>>();
        private int roundId = 0;
        private (int, int) lastPromisedRound = (0, 0); // (round, nodeId)
        private (int, int) lastAcceptedRound = (0, 0); // (round, nodeId)
        private List<Lease> lastAcceptedValue = new List<Lease>();
        private int acceptedCount = 0;

        private List<Lease> valueToPropose = new List<Lease>();

        private List<int> processedInstances = new List<int>();

        public delegate void LeasesUpdate(Dictionary<int, List<Lease>> consensusLog, List<int> processedInstances, bool updateValueToPropose);
        public event LeasesUpdate leasesUpdateRequest;


        public PaxosNode(int id)
        {
            this.id = id;
        }

        public void setClusterNodes(Dictionary<int, GrpcChannel> channels)
        {
            foreach (KeyValuePair<int, GrpcChannel> pair in channels)
            {
                paxosClusterNodes[pair.Key] = new PaxosNodeService.PaxosNodeServiceClient(pair.Value);
            }
        }

        public void setValueToPropose(List<Lease> value)
        {
            valueToPropose = value;
        }

        public async void broadcastPrepare()
        {
            roundId++;

            PrepareRequest request = new PrepareRequest
            {
                Id = this.id,
                RoundId = roundId,
            };

            List<Task<PrepareResponse>> responseTasks = new List<Task<PrepareResponse>>();

            foreach (KeyValuePair<int, PaxosNodeService.PaxosNodeServiceClient> pair in paxosClusterNodes)
            {
                Task<PrepareResponse> response = pair.Value.PrepareAsync(request).ResponseAsync;
                responseTasks.Add(response);
            }

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
                    roundId = Math.Max(roundId, response.LastPromisedRoundId);
                    if (nacksCount > paxosClusterNodes.Count / 2)
                    {
                        broadcastPrepare();
                        return;
                    }
                }
                else
                {
                    promisesCount++;
                    if (response.AcceptedRoundId > lastAcceptedRound.Item1 || (response.AcceptedRoundId == lastAcceptedRound.Item1 && response.AcceptedNodeId > lastAcceptedRound.Item2))
                    {
                        lastAcceptedRound = (response.AcceptedRoundId, response.AcceptedNodeId);
                        lastAcceptedValue = response.AcceptedValue; // FIXME:
                    }
                    if (promisesCount > paxosClusterNodes.Count / 2)
                    {
                        broadcastAccept();
                        return;
                    }
                }
            }
        }

        public async void broadcastAccept()
        {
            if (lastAcceptedRound.Item1 == 0)
            {
                // raise an event that triggers LeaseManager to get value to propose
                leasesUpdateRequest(consensusLog, processedInstances, true);
            }
            else
            {
                valueToPropose = lastAcceptedValue;
            }

            AcceptRequest request = new AcceptRequest
            {
                Id = this.id,
                RoundId = roundId,
                Value = valueToPropose, // FIXME:
            };

            List<Task<AcceptResponse>> responseTasks = new List<Task<AcceptResponse>>();

            foreach (KeyValuePair<int, PaxosNodeService.PaxosNodeServiceClient> pair in paxosClusterNodes)
            {
                Task<AcceptResponse> response = pair.Value.AcceptAsync(request).ResponseAsync;
                responseTasks.Add(response);
            }

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
                    roundId = Math.Max(roundId, response.LastPromisedRoundId);
                    if (nacksCount > paxosClusterNodes.Count / 2)
                    {
                        broadcastPrepare();
                        return;
                    }
                }
                else
                {
                    acceptsCount++;
                    if (acceptsCount > paxosClusterNodes.Count / 2)
                    {
                        return;
                    }
                }
            }
        }

        public void broadcastAccepted()
        {
            AcceptedRequest request = new AcceptedRequest
            {
                Id = this.id,
                RoundId = roundId,
                Value = valueToPropose, // FIXME:
            };

            foreach (KeyValuePair<int, PaxosNodeService.PaxosNodeServiceClient> pair in paxosClusterNodes)
            {
                AcceptedResponse response = pair.Value.Accepted(request);
            }
        }

        public (int, int) getLastPromisedRoundId()
        {
            return lastPromisedRound;
        }

        public void setLastPromisedRound((int, int) roundId)
        {
            lastPromisedRound = roundId;
        }

        public (int, int) getLastAcceptedRoundId()
        {
            return lastAcceptedRound;
        }

        public void setLastAcceptedRound((int, int) roundId)
        {
            lastAcceptedRound = roundId;
        }

        public List<Lease> getLastAcceptedValue()
        {
            return lastAcceptedValue;
        }

        public void setLastAcceptedValue(List<Lease> value)
        {
            lastAcceptedValue = value;
        }

        public int incrAcceptedCount()
        {
            acceptedCount++;
            return acceptedCount;
        }

        public int getAcceptedCount()
        {
            return acceptedCount;
        }

        public int getClusterSize()
        {
            return paxosClusterNodes.Count;
        }

        public void instanceDecided()
        {
            consensusLog[roundId] = valueToPropose;
            leasesUpdateRequest(consensusLog, processedInstances, false);
            processedInstances.Add(roundId);
        }
    }
}