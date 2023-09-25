using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LeaseManager
{
    internal class LeaseManager
    {
        private string id;
        private string url;
        private bool debug = true;

        public LeaseManager(string id, string url)
        {
            this.id = id;
            this.url = url;

            this.Logger("created");
        }

        private void Logger(string message)
        {
            if (debug)
            {
                Console.WriteLine($"[LeaseManager {this.id}]\t" + message + '\n');
            }
        }
    }
}
