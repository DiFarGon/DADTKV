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
        private string id;
        private string url;
        private bool debug = true;


        public TransactionManager(string id, string url)
        {
            this.id = id;
            this.url = url;

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
