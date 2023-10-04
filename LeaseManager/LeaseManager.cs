using System;
using System.Collections.Generic;
using System.Dynamic;
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
        private int clusterId;
        private string id;
        private string url;
        private bool debug;

        // private Dictionary<int, (DateTime, bool)> nodeIds_lastHeartbeat = new Dictionary<int, (DateTime, bool)>(); // used for detecting failures or msgs delays, item2 of the value is to keep track of the respective node's state (suspected or not)

        private int paxosNodesCount = 1;
        private Dictionary<int, (string, LeaseManagerService.LeaseManagerServiceClient)> ids_lmsServices =
            new Dictionary<int, (string, LeaseManagerService.LeaseManagerServiceClient)>();

        private (int, int) readTS = (0, 0); // (roundId, nodeId)
        private (int, int) writeTS = (0, 0); // (roundId, nodeId)
        private List<string> lastAcceptedValue = new List<string>();
        private int promisesReceivedCount = 0;
        private int acceptedReceivedCount = 0;
        private bool proposer = false;
        private bool idle = true;

        private Dictionary<int, (string, TransactionManagerService.TransactionManagerServiceClient)> ids_tmsServices =
            new Dictionary<int, (string, TransactionManagerService.TransactionManagerServiceClient)>();

        // lease have this fromat: "tmId-dataKey1;dataKey2;dataKey3;"
        private Dictionary<string, string> granted_rqstdConflicts = new Dictionary<string, string>(); // key is a granted lease, value is a the lease in queue that conflicts with it ; TODO: maybe could have multiple values?
        private Dictionary<string, List<string>> queue_grntdConflicts = new Dictionary<string, List<string>>(); // key is a lease in queue, value is a list of granted leases that conflict with it
        // this was, in my opinion, the best way to do it.
        //  - for a data key request i check if it conflicts with any of the keys in granted_rqstdConflicts
        //      - if so: 
        //          - add the incoming lease to the values of granted_rqstdConflicts for that all the keys that conflict;
        //          - add the incoming lease to the queue_grntdConflicts and set its value to all the leases in granted_rqstdConflicts that conflict with it;
        //          - send a message to the TM that requested the lease to notify it that it has to wait for the lease; 
        //          - send messages to the TMs that have the leases in granted_rqstdConflicts(keys) that conflict with the incoming lease telling them to execute one more transaction and then release the lease;
        //      - if not:
        //          - add the incoming lease to the queue_grntdConflicts and set its value to an empty list;
        // the big advantage of this implementation is that when a TM releases a lease, i can lookup what key in granted_rqstdConflicts it refers to and obtain the associated value (the lease in queue that conflicts with it)
        // and then remove the released lease from the list of conflicting leases of the lease in queue and check if the list is empty.


        public LeaseManager(int clusterId, string id, string url, bool debugMode)
        {
            this.clusterId = clusterId;
            this.id = id;
            this.url = url;
            this.debug = debugMode;

            // TODO: no taking into account the fact that the leader might change due to timeouts
            if (clusterId == 0) this.proposer = true;

            this.Logger("created");
        }

        public void Logger(string message)
        {
            if (this.debug) Console.WriteLine($"(TimeStamp: {DateTime.UtcNow}): [ LM {this.id} (P{this.clusterId}) ]\t" + message + '\n');
        }

        public bool isIdle() { return this.idle; }

        public bool isProposer() { return this.proposer; }

        public int getClusterId() { return this.clusterId; }

        public string getId() { return this.id; }

        public (int, int) getReadTS() { return this.readTS; }

        public (int, int) getWriteTS() { return this.writeTS; }

        public List<string> getLastAcceptedValue() { return this.lastAcceptedValue; }

        public int getPromisesReceivedCount() { return this.promisesReceivedCount; }

        public int getAcceptedReceivedCount() { return this.acceptedReceivedCount; }

        public void setLastPromisedRound(int round, int nodeId) { this.readTS = (round, nodeId); }

        public void setLastAcceptedRound(int round, int nodeId) { this.writeTS = (round, nodeId); }

        //  FIXME: the data structure of the value is stil tbd
        public void setLastAcceptedValue(List<string> value) { this.lastAcceptedValue = value; }

        public void setPaxosClusterNodes(string lms)
        {
            string[] keyValuePairs = lms.Split(';', StringSplitOptions.RemoveEmptyEntries);

            foreach (string pair in keyValuePairs)
            {
                string[] parts = pair.Split('-');
                int n = int.Parse(parts[0]);
                string id = parts[1];
                string url = parts[2];

                GrpcChannel channel = GrpcChannel.ForAddress(url);
                LeaseManagerService.LeaseManagerServiceClient client = new LeaseManagerService.LeaseManagerServiceClient(channel);
                this.ids_lmsServices[n] = (id, client);

                this.paxosNodesCount++;
            }
            this.Logger($"set lease managers, cluster with {this.paxosNodesCount} nodes");
        }

        public void setTmClusterNodes(string tms)
        {
            string[] keyValuePairs = tms.Split(';', StringSplitOptions.RemoveEmptyEntries);

            foreach (string pair in keyValuePairs)
            {
                string[] parts = pair.Split('-');
                int n = int.Parse(parts[0]);
                string id = parts[1];
                string url = parts[2];

                GrpcChannel channel = GrpcChannel.ForAddress(url);
                TransactionManagerService.TransactionManagerServiceClient client = new TransactionManagerService.TransactionManagerServiceClient(channel);
                this.ids_tmsServices[n] = (id, client);
            }
            this.Logger("set lease managers");
        }

        static public string parseLease(string tmId, List<string> dataKeys)
        {
            string lease = tmId + "-";
            foreach (string key in dataKeys)
            {
                lease += key + ";";
            }
            return lease;
        }

        public void addLeaseToQueue(string tmId, List<string> dataKeys)
        //  FIXME: not doing any checks on the lease, should i?
        {
            string newLease = parseLease(tmId, dataKeys);
            this.queue_grntdConflicts[newLease] = new List<string>();

            foreach (KeyValuePair<string, string> pair in this.granted_rqstdConflicts)
            {
                if (newLease == pair.Key)
                {
                    this.granted_rqstdConflicts[pair.Key] = newLease;
                    this.queue_grntdConflicts[newLease].Add(pair.Key);
                }
            }
        }

        public void moveLeaseQueueToGranted(string tmId, List<string> dataKeys)
        //  FIXME: not doing any checks on the lease, should i?
        {
            string newLease = parseLease(tmId, dataKeys);

            this.queue_grntdConflicts.Remove(newLease);
            this.granted_rqstdConflicts[newLease] = "";
        }

        public void releaseLease(string tmId, List<string> dataKeys)
        //  FIXME: not doing any checks on the lease, should i?
        {
            string releasedLease = parseLease(tmId, dataKeys);

            string blockedLease = this.granted_rqstdConflicts[releasedLease];
            this.granted_rqstdConflicts.Remove(releasedLease);

            this.queue_grntdConflicts[blockedLease].Remove(releasedLease);
        }

        // TODO: should i seperate the incrementation from the checking if quorom?
        public bool incPromiseCountCheckIfQuorom()
        {
            this.promisesReceivedCount++;
            return this.promisesReceivedCount > this.paxosNodesCount / 2;
        }

        // TODO: should i seperate the incrementation from the checking if quorom?
        public bool incAcceptedCountCheckIfQuorom()
        {
            this.acceptedReceivedCount++;
            return this.acceptedReceivedCount > this.paxosNodesCount / 2;
        }

        public void resetPaxosInstance()
        {
            this.promisesReceivedCount = 0;
            this.acceptedReceivedCount = 0;
            // this.readTS = (0, 0);
            // this.writeTS = (0, 0);

        }

        private async Task<bool> sendPrepareMsgs()
        {
            this.Logger("Prepare");
            this.readTS.Item1 = this.readTS.Item1 + 1;

            int majority = (this.paxosNodesCount + 1) / 2; // Assuming the current node is also part of the quorum
            int receivedPromises = 1;

            List<Task<PrepareResponse>> responseTasks = new List<Task<PrepareResponse>>();

            PrepareRequest request = new PrepareRequest
            {
                LmId = this.clusterId,
                RoundId = this.readTS.Item1
            };

            //  Sends prepare messages to all nodes in the cluster and stores tasks in a list to then await each one (not in order)
            foreach (KeyValuePair<int, (string, LeaseManagerService.LeaseManagerServiceClient)> pair in this.ids_lmsServices)
            {
                Task<PrepareResponse> response = pair.Value.Item2.PrepareAsync(request).ResponseAsync;
                responseTasks.Add(response);
            }

            // Loop that awaits for a majority of promises or a nack
            while (true)
            {
                Task<PrepareResponse> completedTask = await Task.WhenAny(responseTasks);
                responseTasks.Remove(completedTask);

                PrepareResponse response = await completedTask;

                if (response.Ack)
                {
                    if (response.LastAcceptedRound > this.writeTS.Item1)
                    {
                        this.writeTS = (response.LastAcceptedRound, response.LastAcceptedRoundNodeId);
                        this.lastAcceptedValue = response.LastAcceptedValue.ToList(); // FIXME: value type tbd
                    }
                    receivedPromises++;
                }
                else
                {
                    this.readTS.Item1 = response.LastPromisedRound;
                    return await this.sendPrepareMsgs();
                }

                if (receivedPromises >= majority)
                {
                    return true;
                }
            }
        }

        private void sendAcceptMsgs()
        {
            this.Logger("Accept");

            AcceptRequest request = new AcceptRequest
            {
                LmId = this.clusterId,
                RoundId = this.readTS.Item1,
                Value = { this.lastAcceptedValue }
            };

            //  Sends prepare messages to all nodes in the cluster and stores tasks in a list to then await each one (not in order)
            foreach (KeyValuePair<int, (string, LeaseManagerService.LeaseManagerServiceClient)> pair in this.ids_lmsServices)
            {
                Task<AcceptResponse> response = pair.Value.Item2.AcceptAsync(request).ResponseAsync;
            }
        }

        public void broadcastAcceptedMsg()
        {
            this.Logger("Broadcast accepted");

            AcceptedRequest request = new AcceptedRequest
            {
                LmId = this.clusterId,
                RoundId = this.readTS.Item1,
                Value = { this.lastAcceptedValue }
            };

            foreach (KeyValuePair<int, (string, LeaseManagerService.LeaseManagerServiceClient)> pair in this.ids_lmsServices)
            {
                AcceptedResponse response = pair.Value.Item2.Accepted(request);
            }
        }

        public async void propose()
        {
            this.Logger("initiate propose");

            bool prepareResult = await this.sendPrepareMsgs();

            if (prepareResult == true) this.sendAcceptMsgs();

            else
            {
                // TODO: if prepare fails it means that another node started 
            }



        }

        // internal void setLastHearbeat(int nodeId)
        // {
        //     this.nodeIds_lastHeartbeat[nodeId] = DateTime.Now;
        // }

        // internal void checkHeartbeats(TimeSpan interval)
        // {
        //     while (true)
        //     {
        //         foreach (KeyValuePair<int, (DateTime, bool)> pair in this.nodeIds_lastHeartbeat)
        //         {
        //             if (DateTime.Now - pair.Value.Item1 > interval) this.nodeIds_lastHeartbeat[pair.Key] = (pair.Value.Item1, true);
        //         }
        //     }
        // }
    }
}
