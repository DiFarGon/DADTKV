using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Client
{
    internal class Client
    {
        private string id;
        private string script;
        private bool debug = true;

        public Client(string id, string script)
        {
            this.id = id;
            this.script = script;

            this.Logger("created");
        }

        private void Logger(string message)
        {
            if (debug)
            {
                Console.WriteLine($"[Client {this.id}]\t" + message + '\n');
            }
        }
    }
}
