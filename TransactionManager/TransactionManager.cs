using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Grpc.Net.Client;
using System.Transactions;
using Lease;

namespace TransactionManager
{
    internal class TransactionManager
    {
        private int clusterId;
        private string id;
        private string url;
        private bool debug;
        private Dictionary<int, (string, TransactionManagerService.TransactionManagerServiceClient)> ids_tmServices =
            new Dictionary<int, (string, TransactionManagerService.TransactionManagerServiceClient)>();
        private Dictionary<int, (string, LeaseManagerService.LeaseManagerServiceClient)> ids_lmServices =
            new Dictionary<int, (string, LeaseManagerService.LeaseManagerServiceClient)>();

        private List<Lease> currentLeases = new List<Lease>();

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
            this.id = id;
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
                Console.WriteLine($"[TransactionManager {this.id}]\t" + message + '\n');
            }
        }

        /// <returns>the id of this Transaction Manager instance</returns>
        public string GetId()
        {
            return this.id;
        }

        /// <returns>a list of every LeaseManagerService</returns>
        public Dictionary<int, (string, LeaseManagerService.LeaseManagerServiceClient)> GetLeaseManagersServices()
        {
            return this.ids_lmServices;
        }

        /// <summary>
        /// Decodes the given string representing a list of Transaction Managers
        /// and saves the decoded list to this.ids_tmServices
        /// </summary>
        /// <param name="tms"></param>
        public void SetTmClusterNodes(string tms)
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
                this.ids_tmServices[n] = (id, client);
            }
            this.Logger("set transaction managers");
        }

        /// <summary>
        /// Decodes the given string representing a list of Lease Managers
        /// and saves the decoded list to this.ids_lmServices
        /// </summary>
        /// <param name="tms"></param>
        public void SetLmClusterNodes(string lms)
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
                this.ids_lmServices[n] = (id, client);
            }
            this.Logger("set lease managers");
        }

        /// <summary>
        /// Sets this Transaction Manager instance list of
        /// known currently assigned leases to the given list
        /// </summary>
        /// <param name="leases"></param>
        public void SetCurrentLeases(List<Lease> leases)
        {
            this.currentLeases = leases;
        }
    }
}