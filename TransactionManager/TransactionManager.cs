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

        public Dictionary<int, (string, TransactionManagerService.TransactionManagerServiceClient )> getTransactionManagersServices()
        {
            return this.ids_tmServices;
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

        //do i have to wait for the replies, do the LM and TM even have to send a reply?
        public void sendStatusRequests() 
        {
            foreach (int clusterId in this.getLeaseManagersServices().Keys)
            {
                LeaseManagerService.LeaseManagerServiceClient channel = this.getLeaseManagersServices()[clusterId].Item2;
                channel.Status_LM(new StatusRequest_LM { });
            }

            foreach (int clusterId in this.getTransactionManagersServices().Keys)
            {
                TransactionManagerService.TransactionManagerServiceClient channel = this.getTransactionManagersServices()[clusterId].Item2;
                channel.Status_TM(new StatusRequest_TM { });
            }
        }
    }
}