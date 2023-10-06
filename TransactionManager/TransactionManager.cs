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
        private string id;
        private string url;
        private bool debug;
        private Dictionary<int, (string, TransactionManagerService.TransactionManagerServiceClient)> ids_tmServices =
            new Dictionary<int, (string, TransactionManagerService.TransactionManagerServiceClient)>();
        private Dictionary<int, (string, LeaseManagerService.LeaseManagerServiceClient)> ids_lmServices =
            new Dictionary<int, (string, LeaseManagerService.LeaseManagerServiceClient)>();

        public TransactionManager(int clusterId, string id, string url, bool debug)
        {
            this.clusterId = clusterId;
            this.id = id;
            this.url = url;
            this.debug = debug;

            this.Logger("created");
        }

        public void Logger(string message)
        {
            if (debug)
            {
                Console.WriteLine($"[TransactionManager {this.id}]\t" + message + '\n');
            }
        }

        public string getId()
        {
            return this.id;
        }

        public Dictionary<int, (string, LeaseManagerService.LeaseManagerServiceClient)> getLeaseManagersServices()
        {
            return this.ids_lmServices;
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
                this.ids_tmServices[n] = (id, client);
            }
            this.Logger("set transaction managers");
        }

        public void setLmClusterNodes(string lms)
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
    }
}