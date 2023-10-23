using Grpc.Net.Client;

namespace TransactionManager
{
    internal class TransactionManager
    {
        private int clusterId;
        public readonly string Id;
        private string url;
        private bool debug;
        public readonly Dictionary<string, TransactionManagerService.TransactionManagerServiceClient> TmServices =
            new Dictionary<string, TransactionManagerService.TransactionManagerServiceClient>();
        public readonly Dictionary<string, LeaseManagerService.LeaseManagerServiceClient> LmServices =
            new Dictionary<string, LeaseManagerService.LeaseManagerServiceClient>();
        private List<Lease.Lease> currentLeases = new List<Lease.Lease>();
        private List<Lease.Lease> heldLeases = new List<Lease.Lease>();
        private List<(Transaction.Transaction, TaskCompletionSource<TransactionResponse> tcs)> pendingTransactions = new List<(Transaction.Transaction, TaskCompletionSource<TransactionResponse> tcs)>();
        private Dictionary<string, DadInt.DadInt> store = new Dictionary<string, DadInt.DadInt>();

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
        /// Decodes the given string representing a list of Transaction Managers
        /// and saves the decoded list to this.TmServices
        /// </summary>
        /// <param name="tms"></param>
        public void SetTmClusterNodes(string tms)
        {
            string[] keyValuePairs = tms.Split(';', StringSplitOptions.RemoveEmptyEntries);

            foreach (string pair in keyValuePairs)
            {
                string[] parts = pair.Split('-');
                string id = parts[1];
                string url = parts[2];

                GrpcChannel channel = GrpcChannel.ForAddress(url);
                TransactionManagerService.TransactionManagerServiceClient client = new TransactionManagerService.TransactionManagerServiceClient(channel);
                this.TmServices[id] = client;
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
            string[] keyValuePairs = lms.Split(';', StringSplitOptions.RemoveEmptyEntries);

            foreach (string pair in keyValuePairs)
            {
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
            this.currentLeases = leases;
            this.UpdateHeldLeases();
            this.ReleaseConflictingLeases();
        }

        /// <summary>
        /// Updates held Leases list, by checking if any Lease in the current leases
        /// list is owned by this Transaction Manager but isn't yet in the held
        /// leases list
        /// </summary>
        private void UpdateHeldLeases()
        {
            // FIXME: what if theres a conflicting lease in the current leases list
            //        when this update happens?
            //        if we immediately consider this lease as held concurrency problems arise
            //        who gets the lease and who doesn't?
            //        do lease managers even assign conflicting leases for the same epoch
            //        in the first place?
            //        so many questions
            foreach(Lease.Lease lease in this.currentLeases)
            {
                if (this.heldLeases.Contains(lease)) continue;
                if (this.Id == lease.TmId) heldLeases.Add(lease);
            }
        }

        /// <summary>
        /// Detects if there are any currently held Leases conflicting with
        /// newly assigned Leases and in the affirmative case releases it
        /// after attempting to execute a single transaction
        /// </summary>
        private void ReleaseConflictingLeases()
        {
            foreach(Lease.Lease lease in this.heldLeases)
            {
                if (lease.ConflictsWithAny(this.currentLeases))
                {
                    this.AttemptOnlyFirstTransaction();
                    this.heldLeases.Remove(lease);
                }
            }
        }

        /// <returns>A list with every key held through a lease</returns>
        public List<string> KeysHeld()
        {
            List<string> keys = new List<string>();
            foreach (Lease.Lease lease in this.heldLeases)
            {
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
            this.Logger("Staging transaction");
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
            (Transaction.Transaction transaction, TaskCompletionSource<TransactionResponse> tcs)  = this.pendingTransactions[1];
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
            this.Logger("Broadcasting lease request to lease managers");

            LeaseRequest leaseRequest = new LeaseRequest { TmId = this.Id };
            
            List<string> keysWrite = new List<string>();
            foreach (DadInt.DadInt dadInt in transaction.DadIntsWrite)
            {
                keysWrite.Add(dadInt.Key);
            }

            List<string> keys = transaction.ReadKeys.Concat(keysWrite).ToList();

            foreach (string key in keys) { leaseRequest.Keys.Add(key); }

            foreach (LeaseManagerService.LeaseManagerServiceClient leaseManagerService in this.LmServices.Values)
            {
                LeaseManagerService.LeaseManagerServiceClient channel = leaseManagerService;
                channel.Lease(leaseRequest);
            }
        }

        /// <summary>
        /// Writes the changes to be made by this transaction to this Transaction
        /// Manager store
        /// </summary>
        /// <param name="transaction"></param>
        public void WriteTransactionToStore(Transaction.Transaction transaction)
        {
            this.Logger("Writing transaction to store");

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
            this.Logger("Executing transaction");

            this.WriteTransactionToStore(transaction);
            List<DadInt.DadInt> readDadInts = new List<DadInt.DadInt>();
            foreach (string key in transaction.ReadKeys)
            {
                readDadInts.Add(this.store[key]);
            }
            this.CommunicateTransactionExecuted(transaction);
            return readDadInts;
        }

        /// <summary>
        /// Communicates the execution of a transaction to other Transaction Managers
        /// </summary>
        /// <param name="transaction"></param>
        public void CommunicateTransactionExecuted(Transaction.Transaction transaction)
        {
            this.Logger("Broadcasing executed transaction to every Transaction Manager");

            foreach (TransactionManagerService.TransactionManagerServiceClient service in this.TmServices.Values)
            {
                TransactionMessage transactionMessage = new TransactionMessage();
                foreach (DadInt.DadInt dadInt in transaction.DadIntsWrite)
                {
                    transactionMessage.DadIntsWrite.Add(new DadIntMessage
                        {
                            Key = dadInt.Key,
                            Value = dadInt.Value
                        });
                }
                foreach (string key in transaction.ReadKeys)
                {
                    transactionMessage.KeysRead.Add(key);
                }
                TransactionExecutedRequest request = new TransactionExecutedRequest();
                request.TransactionMessage = transactionMessage;
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
            this.Logger("Attempting transaction");

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