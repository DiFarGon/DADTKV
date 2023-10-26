using Grpc.Net.Client;

namespace TransactionManager
{
    internal class TransactionManager
    {
        private int clusterId;
        public readonly string Id;
        private string url;
        private bool debug;

        private Dictionary<string, int> tmsIds_tmsClusterIds = new Dictionary<string, int>();
        public readonly Dictionary<string, TransactionManagerService.TransactionManagerServiceClient> TmServices =
            new Dictionary<string, TransactionManagerService.TransactionManagerServiceClient>();
        public readonly Dictionary<string, LeaseManagerService.LeaseManagerServiceClient> LmServices =
            new Dictionary<string, LeaseManagerService.LeaseManagerServiceClient>();
        private List<Lease.Lease> currentLeases = new List<Lease.Lease>();
        private List<Lease.Lease> heldLeases = new List<Lease.Lease>();
        private List<(Transaction.Transaction, TaskCompletionSource<TransactionResponse> tcs)> pendingTransactions = new List<(Transaction.Transaction, TaskCompletionSource<TransactionResponse> tcs)>();
        private Dictionary<string, DadInt.DadInt> store = new Dictionary<string, DadInt.DadInt>();
        private Dictionary<int, List<string>> failureSuspicions = new Dictionary<int, List<string>>();
        private List<string> suspected = new List<string>();
        public int CrashTimeSlot = -1;

        /// <summary>
        /// Creates a new Transaction Manager with given parameters
        /// </summary>
        /// <param name="clusterId"></param>
        /// <param name="id"></param>
        /// <param name="url"></param>
        /// <param name="debug"></param>
        public TransactionManager(int clusterId, string id, string url, bool debug)
        {
            this.clusterId = clusterId;
            this.Id = id;
            this.url = url;
            this.debug = debug;

            this.Logger("Created");
        }

        /// <summary>
        /// Logs message prefixing it with an identifier
        /// </summary>
        /// <param name="message"></param>
        public void Logger(string message)
        {
            if (debug)
            {
                Console.WriteLine($"[TransactionManager {this.Id}]\t" + message + '\n');
            }
        }

        /// <summary>
        /// Initializes this Transaction Manager crashTimeSlot and suspicions properties
        /// by reading the configuration file
        /// </summary>
        public void ConfigureStateAndSuspicions(string configFile)
        {
            Dictionary<int, List<string>> suspicions = new Dictionary<int, List<string>>();
            using StreamReader reader = new StreamReader(configFile);
            {
                string? line;
                while ((line = reader.ReadLine()) != null)
                {
                    if (line.StartsWith("F"))
                    {
                        string[] parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                        int timeSlot = int.Parse(parts[1]);
                        int tmsStatesStartIndex = 2;
                        if (parts[tmsStatesStartIndex + clusterId] == "C")
                            this.CrashTimeSlot = timeSlot;

                        for (int i = tmsStatesStartIndex + TmServices.Count + LmServices.Count; i < parts.Length; i++)
                        {
                            string[] sus = parts[i].Trim('(', ')').Split(',');
                            if (sus[0] == Id)
                            {
                                if (suspicions.ContainsKey(timeSlot))
                                    suspicions[timeSlot].Add(sus[1]);
                                else
                                {
                                    suspicions[timeSlot] = new List<string>
                                        {
                                            sus[1]
                                        };
                                }
                            }
                        }
                    }
                }
            }
            this.failureSuspicions = suspicions;

            foreach (KeyValuePair<int, List<string>> entry in suspicions)
            {
                this.Logger($"suspicions at time slot {entry.Key}: {string.Join(", ", entry.Value)}");
            }
        }

        /// <summary>
        /// Sets the current suspicions list to the adequate list for the current epoch
        /// </summary>
        /// <param name="currentEpoch"></param>
        public void SetCurrentSuspicions(int currentEpoch)
        {
            if (!this.failureSuspicions.Keys.Contains(currentEpoch))
            {
                this.suspected = new List<string>();
                return;
            }
            this.suspected = this.failureSuspicions[currentEpoch];
        }

        /// <summary>
        /// Decodes the given string representing a list of Transaction Managers
        /// and saves the decoded list to this.TmServices
        /// </summary>
        /// <param name="tms"></param>
        public void SetTmClusterNodes(string tms)
        {
            string[] keyValuePairs = tms.Split('!', StringSplitOptions.RemoveEmptyEntries);

            foreach (string pair in keyValuePairs)
            {
                string[] parts = pair.Split('-');
                int n = int.Parse(parts[0]);
                string id = parts[1];
                string url = parts[2];

                GrpcChannel channel = GrpcChannel.ForAddress(url);
                TransactionManagerService.TransactionManagerServiceClient client = new TransactionManagerService.TransactionManagerServiceClient(channel);
                this.TmServices[id] = client;
                this.tmsIds_tmsClusterIds[id] = n;
            }
            this.Logger("Set transaction managers");
        }

        /// <summary>
        /// Decodes the given string representing a list of Lease Managers
        /// and saves the decoded list to this.LmServices
        /// </summary>
        /// <param name="tms"></param>
        public void SetLmClusterNodes(string lms)
        {
            string[] keyValuePairs = lms.Split('!', StringSplitOptions.RemoveEmptyEntries);

            int count = 0;

            foreach (string pair in keyValuePairs)
            {
                count++;

                string[] parts = pair.Split('-');
                string id = parts[1];
                string url = parts[2];

                GrpcChannel channel = GrpcChannel.ForAddress(url);
                LeaseManagerService.LeaseManagerServiceClient client = new LeaseManagerService.LeaseManagerServiceClient(channel);
                this.LmServices[id] = client;
            }
            this.Logger("Set lease managers");
        }

        /// <summary>
        /// Sets this Transaction Manager instance list of
        /// known currently assigned leases to the given list
        /// </summary>
        /// <param name="leases"></param>
        public void SetCurrentLeases(List<Lease.Lease> leases)
        {
            this.Logger("Setting current leases list");
            this.currentLeases.AddRange(leases);
            this.UpdateHeldLeases();
        }

        /// <summary>
        /// Updates held Leases list, by checking if any Lease in the current leases
        /// list is owned by this Transaction Manager but isn't yet in the held
        /// leases list. In case there are conflicting Leases in the current leases
        /// list, the one with the lower index prevails. After updating held leases
        /// checks if the new held leases list contains leases to be released and
        /// attempts to execute transactions
        /// </summary>
        private void UpdateHeldLeases()
        {
            this.Logger("Updating currently held leases");

            foreach (Lease.Lease lease in this.currentLeases)
            {
                if (this.heldLeases.Contains(lease)) continue;
                this.Logger($"lease id: {lease.TmId} my id: {this.Id}");
                if (this.Id == lease.TmId)
                {
                    this.Logger("same ids");
                    List<Lease.Lease> conflictingLeases = lease.ConflictsWithAny(this.currentLeases);
                    foreach (Lease.Lease conflictingLease in conflictingLeases)
                    {
                        if (this.currentLeases.IndexOf(lease) < this.currentLeases.IndexOf(conflictingLease))
                        {
                            this.heldLeases.Add(lease);
                        }
                    }
                    if (conflictingLeases.Count() == 0)
                    {
                        this.heldLeases.Add(lease);
                    }
                }
            }
            this.ReleaseConflictingLeases();
            this.AttemptFirstTransaction();
        }

        /// <summary>
        /// Detects if there are any currently held Leases conflicting with
        /// newly assigned Leases and in the affirmative case releases it
        /// after attempting to execute a single transaction
        /// </summary>
        private void ReleaseConflictingLeases()
        {
            this.Logger("Checking if there are conflicting leases");
            foreach (Lease.Lease lease in this.heldLeases)
            {
                if (lease.ConflictsWithAny(this.currentLeases).Count != 0)
                {
                    this.Logger("Releasing conflicting lease:" + lease.ToString() + "\n");
                    this.AttemptOnlyFirstTransaction();
                    this.heldLeases.Remove(lease);
                    this.currentLeases.Remove(lease);
                    this.CommunicateLeaseReleased(lease);
                }
            }
        }

        /// <summary>
        /// Communicates the releasing of a lease to every other Transaction Manager instance
        /// </summary>
        /// <param name="lease"></param>
        private void CommunicateLeaseReleased(Lease.Lease lease)
        {
            this.Logger($"Informing other Transaction Managers lease {lease.ToString()} was released");

            LeaseMessageTM message = lease.ToLeaseMessage();
            LeaseReleasedRequest request = new LeaseReleasedRequest();
            request.LeaseMessage = message;
            foreach (TransactionManagerService.TransactionManagerServiceClient service in this.TmServices.Values)
            {
                service.LeaseReleased(request);
            }
        }

        /// <summary>
        /// Removes Lease from current leases list and checks if any 
        /// </summary>
        /// <param name="lease"></param>
        public void RemoveLease(Lease.Lease lease)
        {
            this.Logger($"Removing lease {lease.ToString()}");

            this.currentLeases.Remove(lease);
            this.UpdateHeldLeases();
        }

        /// <returns>A list with every key held through a lease</returns>
        public List<string> KeysHeld()
        {
            List<string> keys = new List<string>();
            foreach (Lease.Lease lease in this.heldLeases)
            {
                this.Logger("Lease: " + lease.ToString());
                keys = keys.Union(lease.Keys).ToList();
            }
            return keys;
        }

        /// <summary>
        /// Add transaction to the pending transactions list as well as its corresponding
        /// TaskCompletionSource
        /// </summary>
        /// <param name="transaction"></param>
        public void StageTransaction(Transaction.Transaction transaction, TaskCompletionSource<TransactionResponse> tcs)
        {
            this.Logger($"Staging transaction {transaction.ToString()}");
            this.pendingTransactions.Add((transaction, tcs));
        }

        /// <summary>
        /// Attempts the first transaction, if it succeeds attempts the next
        /// </summary>
        public void AttemptFirstTransaction()
        {
            if (this.AttemptOnlyFirstTransaction()) this.AttemptFirstTransaction();
        }

        /// <summary>
        /// Attempts to execute the first and only the first pending transaction.
        /// If it succeeds sets the TaskCompletionSource.Result to the adequate 
        /// response value and removes the transaction from the pending list
        /// </summary>
        /// <returns>true if it suceeded, false if it failed</returns>
        private bool AttemptOnlyFirstTransaction()
        {
            this.Logger("Attempting first pending transaction");
            (Transaction.Transaction transaction, TaskCompletionSource<TransactionResponse> tcs) = this.pendingTransactions[0];
            (bool, List<DadInt.DadInt>) result = this.AttemptTransaction(transaction);
            if (result.Item1)
            {
                List<DadIntMessage> dadIntMessages = new List<DadIntMessage>();
                foreach (DadInt.DadInt dadInt in result.Item2)
                {
                    dadIntMessages.Add(new DadIntMessage
                    {
                        Key = dadInt.Key,
                        Value = dadInt.Value
                    });
                }
                TransactionResponse response = new TransactionResponse();
                response.Read.AddRange(dadIntMessages);
                tcs.SetResult(response);
                this.pendingTransactions.RemoveAt(0);
                return true;
            }
            return false;
        }

        /// <summary>
        /// Broadcast to all Lease Managers a request for acquiring the required
        /// Lease to execute the given Transaction
        /// </summary>
        /// <param name="transaction"></param>
        public void RequestLease(Transaction.Transaction transaction)
        {
            LeaseRequest leaseRequest = new LeaseRequest { TmId = this.Id };

            List<string> keysWrite = new List<string>();
            foreach (DadInt.DadInt dadInt in transaction.DadIntsWrite)
            {
                keysWrite.Add(dadInt.Key);
            }

            List<string> keys = transaction.ReadKeys.Concat(keysWrite).ToList();
            keys = keys.Distinct().ToList();

            this.Logger("Requesting keys: " + string.Join(", ", keys));
            foreach (string key in keys)
            {
                leaseRequest.Keys.Add(key);
            }

            foreach (LeaseManagerService.LeaseManagerServiceClient leaseManagerService in this.LmServices.Values)
            {
                leaseManagerService.Lease(leaseRequest);
            }
        }

        /// <summary>
        /// Checks if this Transaction Manager suspects of Transaction Manager with id = tmId
        /// </summary>
        /// <param name="tmId"></param>
        /// <returns> true if this Transaction Manager suspects of given one, false otherwise</returns>
        public bool Suspects(string tmId)
        {
            return this.suspected.Contains(tmId);
        }

        /// <summary>
        /// Writes the changes to be made by this transaction to this Transaction
        /// Manager store
        /// </summary>
        /// <param name="transaction"></param>
        public void WriteTransactionToStore(Transaction.Transaction transaction)
        {
            this.Logger($"Writing transaction {transaction.ToString()} to store");

            foreach (DadInt.DadInt dadInt in transaction.DadIntsWrite)
            {
                this.store[dadInt.Key] = dadInt;
            }
        }

        /// <summary>
        /// Executes the given transaction and tells every other Transaction
        /// Manager the transaction was executed
        /// </summary>
        /// <param name="transaction"></param>
        /// <returns>the list of the read DadInts</returns>
        public List<DadInt.DadInt> ExecuteTransaction(Transaction.Transaction transaction)
        {
            this.Logger($"Executing transaction {transaction.ToString()}");

            List<DadInt.DadInt> readDadInts = new List<DadInt.DadInt>();
            foreach (string key in transaction.ReadKeys)
            {
                if (this.store.ContainsKey(key))
                {
                    readDadInts.Add(this.store[key]);
                }
                else
                {
                    readDadInts.Add(new DadInt.DadInt(key, int.MinValue));
                }
            }
            if (this.ProposeTransaction())
            {
                this.WriteTransactionToStore(transaction);
                this.CommunicateTransactionExecuted(transaction);
                return readDadInts;
            }
            List<DadInt.DadInt> returnList = new List<DadInt.DadInt>();
            DadInt.DadInt abortDadInt = new DadInt.DadInt("abort", 0);
            returnList.Add(abortDadInt);
            return returnList;
        }

        /// <summary>
        /// Propose this transaction to other Transaction Managers
        /// </summary>
        /// <param name="transaction"></param>
        private bool ProposeTransaction()
        {
            this.Logger("Proposing transaction");
            int count = 1;
            foreach (TransactionManagerService.TransactionManagerServiceClient service in TmServices.Values)
            {
                ProposeTransactionResponse response = service.ProposeTransaction(new ProposeTransactionRequest { TmId = this.Id });
                if (response.Accept) { count++; }
            }
            return count > (TmServices.Keys.Count() + 1) / 2 ;
        }

        /// <summary>
        /// Communicates the execution of a transaction to other Transaction Managers
        /// </summary>
        /// <param name="transaction"></param>
        public void CommunicateTransactionExecuted(Transaction.Transaction transaction)
        {
            this.Logger($"Broadcasing executed transaction {transaction.ToString()} to every Transaction Manager");

            TransactionExecutedRequest request = new TransactionExecutedRequest();
            TransactionMessage transactionMessage = transaction.ToTransactionMessage();
            request.TransactionMessage = transactionMessage;

            foreach (TransactionManagerService.TransactionManagerServiceClient service in this.TmServices.Values)
            {
                service.TransactionExecuted(request);
            }
        }

        /// <summary>
        /// Checks if all required keys are held through leases and in
        /// the positive case executes the transaction
        /// </summary>
        /// <param name="keysToRead"></param>
        /// <param name="dadIntsToWrite"></param>
        /// <returns>a boolean indicating the success of the operation
        /// and a list with the read DadInts</returns>
        public (bool, List<DadInt.DadInt>) AttemptTransaction(Transaction.Transaction transaction)
        {
            this.Logger($"Attempting transaction {transaction.ToString()}");

            List<string> keysHeld = this.KeysHeld();
            List<string> requiredKeys = transaction.ReadKeys;
            foreach (DadInt.DadInt dadInt in transaction.DadIntsWrite)
            {
                if (!requiredKeys.Contains(dadInt.Key))
                {
                    requiredKeys.Add(dadInt.Key);
                }
            }

            bool allRequiredKeysHeld = requiredKeys.All(element => keysHeld.Contains(element));
            if (allRequiredKeysHeld)
            {
                return (true, this.ExecuteTransaction(transaction));
            }
            return (false, new List<DadInt.DadInt>());
        }
    }
}