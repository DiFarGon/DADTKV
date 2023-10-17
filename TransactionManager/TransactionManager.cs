using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Grpc.Net.Client;
using System.Transactions;

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

            this.Logger("created");
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
            this.Logger("set transaction managers");
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
            this.Logger("set lease managers");
        }

        /// <summary>
        /// Sets this Transaction Manager instance list of
        /// known currently assigned leases to the given list
        /// </summary>
        /// <param name="leases"></param>
        public void SetCurrentLeases(List<Lease.Lease> leases)
        {
            this.currentLeases = leases;
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
        /// Checks if all required keys are held through leases and in
        /// the positive case executes the transaction
        /// </summary>
        /// <param name="keysToRead"></param>
        /// <param name="dadIntsToWrite"></param>
        /// <returns>true if it was possible to execute the transaction,
        /// false if not all required keys were held</returns>
        public bool ExecuteTransaction(List<string> keysToRead, List<DadInt.DadInt> dadIntsToWrite)
        {
            List<string> keysHeld = this.KeysHeld();
            List<string> requiredKeys = keysToRead;
            foreach (DadInt.DadInt dadInt in dadIntsToWrite)
            {
                if (!requiredKeys.Contains(dadInt.Key))
                {
                    requiredKeys.Add(dadInt.Key);
                }
            }

            bool allRequiredKeysHeld = requiredKeys.All(element => keysHeld.Contains(element));
            if (allRequiredKeysHeld)
            {
                // TODO: execute transaction
                return true;
            }
            return false;
        }
    }
}