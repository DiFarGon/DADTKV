using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
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

        private Dictionary<int, string> paxosNodes_ids = new Dictionary<int, string>();

        private Dictionary<string, LeaseManagerService.LeaseManagerServiceClient> ids_lmsServices =
            new Dictionary<string, LeaseManagerService.LeaseManagerServiceClient>();

        private Dictionary<int, string> tmsNodes_ids = new Dictionary<int, string>();

        private Dictionary<string, TransactionManagerService.TransactionManagerServiceClient> ids_tmsServices =
            new Dictionary<string, TransactionManagerService.TransactionManagerServiceClient>();

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

            if (this.debug)
                this.Logger("created");
        }

        private void Logger(string message)
        {
            Console.WriteLine($"[ LM {this.id} (P{this.clusterId}) ]\t" + message + '\n');
        }

        public string getClusterId() { return this.clusterId.ToString(); }

        public string getId() { return this.id; }

        public void setPaxosClusterNodes(string lms)
        {
            string[] keyValuePairs = lms.Split(';', StringSplitOptions.RemoveEmptyEntries);

            foreach (string pair in keyValuePairs)
            {
                string[] parts = pair.Split('-');
                int n = int.Parse(parts[0]);
                string id = parts[1];
                string url = parts[2];

                this.paxosNodes_ids[n] = id;

                GrpcChannel channel = GrpcChannel.ForAddress(url);
                LeaseManagerService.LeaseManagerServiceClient client = new LeaseManagerService.LeaseManagerServiceClient(channel);
                this.ids_lmsServices[id] = client;
            }
            if (this.debug)
                this.Logger("set lease managers");
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

                this.tmsNodes_ids[n] = id;

                GrpcChannel channel = GrpcChannel.ForAddress(url);
                TransactionManagerService.TransactionManagerServiceClient client = new TransactionManagerService.TransactionManagerServiceClient(channel);
                this.ids_tmsServices[id] = client;
            }
            if (this.debug)
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

    }
}
