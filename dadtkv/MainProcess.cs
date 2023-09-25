using System.Diagnostics;

namespace dadtkv
{
    internal class MainProcess
    {
        private bool debug = true;

        private void Logger(string message)
        {
            if (debug)
            {
                Console.WriteLine("[MainProcess]\t" + message + '\n');
            }
        }

        internal void handleP(string line)
        {
            string[] tokens = line.Split(' ');
            string id = tokens[1];
            string type = tokens[2];
            string url = tokens[3];

            switch (type)
            {
                case "T":
                    this.newTransactionManager(id, url);
                    break;
                case "L":
                    this.newLeaseManager(id, url);
                    break;
                case "C":
                    this.newClient(id, url);
                    break;
            }
        }

        internal void handleS(string line)
        {
            string slots = line.Split(' ')[1];

            this.Logger($"Test lasts {slots} slots");
        }

        internal void handleD(string line)
        {
            string time = line.Split(' ')[1];

            this.Logger($"Each slot lasts {time} miliseconds");
        }

        internal void handleT(string line)
        {
            string time = line.Split(' ')[1];

            this.Logger($"Test starts at {time}");
        }

        internal void handleF(string line)
        {
            this.Logger("I don't really want to do this right now");
        }

        private void newTransactionManager(string id, string url)
        {
            this.Logger($"Creating new transaction manager with id '{id}' and url '{url}'");
            Process.Start("../../../../TransactionManager/bin/Debug/net6.0/TransactionManager.exe", $"{id} {url}");
        }

        private void newLeaseManager(string id, string url)
        {
            this.Logger($"Creating new lease manager with id '{id}' and url '{url}'");
            Process.Start("../../../../LeaseManager/bin/Debug/net6.0/LeaseManager.exe", $"{id} {url}");
        }

        private void newClient(string id, string url)
        {
            this.Logger($"Creating new client with id '{id}' and url '{url}'");
            Process.Start("../../../../Client/bin/Debug/net6.0/Client.exe", $"{id} {url}");
        }
    }
}
