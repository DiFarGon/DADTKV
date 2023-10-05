using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TransactionManager
{
    internal class TransactionManager
    {
        private int clusterId;
        private string id;
        private string url;
        private bool debug;


        public TransactionManager(int clusterId, string id, string url, bool debug)
        {
            this.clusterId = clusterId;
            this.id = id;
            this.url = url;
            this.debug = debug;

            this.Logger("created");
        }

        private void Logger(string message)
        {
            if (debug)
            {
                Console.WriteLine($"[TransactionManager {this.id}]\t" + message + '\n');
            }
        }
    }
}