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
        private int clusterId;
        private string id;
        private string url;
        private bool debug;
        private DateTime startTime;

        private Dictionary<int, (DateTime, bool)> nodeIds_lastHeartbeat = new Dictionary<int, (DateTime, bool)>(); // used for detecting failures, item2 of the value is to keep track of the respective node's state (suspected down or not)

        private LeaseManagerService.LeaseManagerServiceClient ownClient; // client that is this server to send accept msgs to himself so that a majority includes himself

        private int paxosNodesCount = 1;
        private Dictionary<int, (string, LeaseManagerService.LeaseManagerServiceClient)> ids_lmsServices =
            new Dictionary<int, (string, LeaseManagerService.LeaseManagerServiceClient)>();

        private (int, int) readTS = (0, 0); // (roundId, nodeId)
        private (int, int) writeTS = (0, 0); // (roundId, nodeId)
        private List<string> lastAcceptedValue = new List<string>();
        private int acceptedReceivedCount = 0;
        private bool proposer = false;
        private bool decided = false;

        private int paxosInstanceId = 0;

        private Dictionary<int, (string, TransactionManagerService.TransactionManagerServiceClient)> ids_tmsServices =
            new Dictionary<int, (string, TransactionManagerService.TransactionManagerServiceClient)>();

        // lease have this fromat: "tmId-dataKey1;dataKey2;dataKey3;"
        private List<Lease> GrantedLeases = new List<Lease>();
        private List<Lease> WaitingLeases = new List<Lease>();


        public LeaseManager(int clusterId, string id, string url, string startTime, bool debugMode)
        {
            this.clusterId = clusterId;
            this.id = id;
            this.url = url;
            this.debug = debugMode;
            this.startTime = DateTime.ParseExact(startTime, "hh:mm:ss", null, DateTimeStyles.None);


            GrpcChannel channel = GrpcChannel.ForAddress(url);
            this.ownClient = new LeaseManagerService.LeaseManagerServiceClient(channel);

            // TODO: no taking into account the fact that the leader might change due to timeouts
            if (clusterId == 0) this.proposer = true;

            this.Logger("created");
        }

        public void Logger(string message)
        {
            if (this.debug) Console.WriteLine($"(TimeStamp: {DateTime.UtcNow}): [ LM {this.id} (P{this.clusterId}) ]\t" + message + '\n');
        }

        /// <summary>
        /// Adds a lease to the queue. If a lease owned by the
        /// same Transaction Manager already exists in the queue,
        /// simply adds new lease's keys which the existing one
        /// didn't contain already. Makes sure every conflict is
        /// adressed after.
        /// </summary>
        /// <param name="newLease">Lease to be added</param>
        public void AddLeaseToQueue(Lease newLease)
        {
            foreach (Lease existingLease in this.WaitingLeases)
            {
                if (existingLease.TmId == newLease.TmId) {
                    existingLease.AddKeys(newLease.Keys);
                    this.HandleLeaseBlockage(existingLease);
                    return;
                }
            }
            this.WaitingLeases.Add(newLease);
            this.HandleLeaseBlockage(newLease);
        }

        /// <summary>
        /// If lease conflicts with an already granted lease adds
        /// given lease to the conflict list of every conflicting 
        /// granted lease and the other way round
        /// </summary>
        /// <param name="lease">Potentially conflicting lease</param>
        private void HandleLeaseBlockage(Lease lease)
        {
            foreach (Lease granted in this.GrantedLeases)
            {
                if (lease.ConflictsWith(granted))
                {
                    granted.AddConflict(lease);
                    lease.AddConflict(granted);
                }
            }
        }
        public bool isProposer() { return this.proposer; }

        public bool isDecided() { return this.decided; }

        public bool setDecided(bool value) { return this.decided = value; }

        public int getClusterId() { return this.clusterId; }

        public (int, int) getReadTS() { return this.readTS; }

        public (int, int) getWriteTS() { return this.writeTS; }

        public List<string> getLastAcceptedValue() { return this.lastAcceptedValue; }

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

                this.nodeIds_lastHeartbeat[n] = (DateTime.Now, true);

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

        public void incAcceptedCount() { this.acceptedReceivedCount++; }

        public bool isAcceptedQuorom() { return this.acceptedReceivedCount > this.paxosNodesCount / 2; }

        public async Task failureDetectorAsync(int controlMsgsInterval, int timeout)
        {
            Thread checkHeartbeatsThread = new Thread(async () => await this.checkHeartbeatsAsync(timeout));
            checkHeartbeatsThread.Start();

            DateTime lastHeartbeat = DateTime.Now;
            List<Task<ControlLMResponse>> responseTasks = new List<Task<ControlLMResponse>>();

            while (true)
            {
                if (DateTime.Now - lastHeartbeat > TimeSpan.FromSeconds(controlMsgsInterval))
                {
                    responseTasks.Clear();
                    ControlLMRequest request = new ControlLMRequest
                    {
                        LmId = this.clusterId
                    };

                    foreach (KeyValuePair<int, (string, LeaseManagerService.LeaseManagerServiceClient)> keyValuePair in this.ids_lmsServices)
                    {
                        Task<ControlLMResponse> res = keyValuePair.Value.Item2.ControlLMAsync(request).ResponseAsync;
                        responseTasks.Add(res);

                        this.nodeIds_lastHeartbeat[keyValuePair.Key] = (DateTime.Now, nodeIds_lastHeartbeat[keyValuePair.Key].Item2);
                    }
                }

                if (responseTasks.Count != 0)
                {
                    Task<ControlLMResponse> completedTask = await Task.WhenAny(responseTasks);
                    responseTasks.Remove(completedTask);

                    ControlLMResponse response = await completedTask;
                    nodeIds_lastHeartbeat[response.LmId] = (DateTime.Now, nodeIds_lastHeartbeat[response.LmId].Item2);
                }
            }
        }

        private Task checkHeartbeatsAsync(int timeout)
        {
            while (true)
            {
                lock (this.nodeIds_lastHeartbeat)
                {
                    foreach (KeyValuePair<int, (DateTime, bool)> pair in this.nodeIds_lastHeartbeat)
                    {
                        if (DateTime.Now - pair.Value.Item1 > TimeSpan.FromSeconds(timeout)) this.nodeIds_lastHeartbeat[pair.Key] = (pair.Value.Item1, false);
                        else this.nodeIds_lastHeartbeat[pair.Key] = (pair.Value.Item1, true);
                    }
                }
                if (this.isLowestNodeAlive()) this.proposer = true;
                else this.proposer = false;
                Thread.Sleep(timeout);
            }
        }

        private bool isLowestNodeAlive()
        {
            foreach (KeyValuePair<int, (DateTime, bool)> pair in this.nodeIds_lastHeartbeat)
            {
                if (pair.Value.Item2 == true && pair.Key < this.clusterId) return false;
            }
            return true;
        }

        private async Task<bool> sendPrepareMsgs()
        {
            this.Logger("Prepare");
            this.readTS.Item1 = this.readTS.Item1 + 1;

            int majority = (this.paxosNodesCount + 1) / 2; // Assuming the current node is also part of the quorum
            int receivedPromises = 1; // starts counting with itself

            List<Task<PrepareResponse>> responseTasks = new List<Task<PrepareResponse>>();

            PrepareRequest request = new PrepareRequest
            {
                LmId = this.clusterId,
                RoundId = this.readTS.Item1
            };

            //  Sends prepare messages to all nodes in the cluster and stores tasks in a list 
            foreach (KeyValuePair<int, (string, LeaseManagerService.LeaseManagerServiceClient)> pair in this.ids_lmsServices)
            {
                Task<PrepareResponse> response = pair.Value.Item2.PrepareAsync(request).ResponseAsync;
                responseTasks.Add(response);
            }

            // Loop that awaits for a majority of promises or a nack
            while (responseTasks.Count > 0)
            {
                Task<PrepareResponse> completedTask = await Task.WhenAny(responseTasks);
                responseTasks.Remove(completedTask);

                PrepareResponse response = await completedTask;

                if (response.Ack)
                {
                    // promise contained a higher wts than the current one, update it and also update the value to be proposed
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
                    return false;
                }

                if (receivedPromises >= majority)
                {
                    return true;
                }
            }
            return false; // not supposed to reach this
        }

        private async Task<bool> sendAcceptMsgs()
        {
            this.Logger("Accept");

            AcceptRequest request = new AcceptRequest
            {
                LmId = this.clusterId,
                RoundId = this.readTS.Item1,
                Value = { this.lastAcceptedValue } // FIXME: the value still TBD
            };

            List<Task<AcceptResponse>> responseTasks = new List<Task<AcceptResponse>>();

            Task<AcceptResponse> responseTask = ownClient.AcceptAsync(request).ResponseAsync;

            responseTasks.Add(responseTask);

            //  Sends prepare messages to all nodes in the cluster and stores tasks in a list to then await each one (not in order)
            foreach (KeyValuePair<int, (string, LeaseManagerService.LeaseManagerServiceClient)> pair in this.ids_lmsServices)
            {
                Task<AcceptResponse> response = pair.Value.Item2.AcceptAsync(request).ResponseAsync;
                responseTasks.Add(response);
            }

            while (responseTasks.Count > 0)
            {
                Task<AcceptResponse> completedTask = await Task.WhenAny(responseTasks);
                responseTasks.Remove(completedTask);

                AcceptResponse response = await completedTask;

                if (!response.Ack)
                {
                    this.readTS.Item1 = response.LastPromisedRoundId;
                    return false;
                }
            }
            return true;
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
            if (await this.sendPrepareMsgs())
            {
                if (await this.sendAcceptMsgs() && this.isAcceptedQuorom())
                    return;
                else
                    this.propose();
            }
            else
                this.propose();
        }

        public void runPaxosInstance()
        {
            // TODO: should i send this instance id to other nodes during paxos in case one is stuck in a past instance?
            this.paxosInstanceId++;
            while (!this.isDecided())
            {
                if (this.isProposer())
                {
                    this.propose();
                }
                else
                    Thread.Sleep(1000);
            }
            this.readTS = (0, 0);
            this.writeTS = (0, 0);
            this.lastAcceptedValue.Clear();
            this.acceptedReceivedCount = 0;
            this.setDecided(false);
            return;
        }
    }
}
