using System.Diagnostics;
using System.Transactions;
using System.Runtime.InteropServices;

namespace dadtkv
{
    internal class MainProcess
    {
        private bool debug;
        private string path;
        private List<ProcessInfo> clients;
        private List<ProcessInfo> transactionManagers;
        private List<ProcessInfo> leaseManagers;

        public MainProcess(bool debug=true)
        {
            this.debug = debug;

            string path = Environment.CurrentDirectory;
            int lastIndex = 0;
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux) || RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                lastIndex = path.LastIndexOf('/');
            }
            else
            {
                lastIndex = path.LastIndexOf('\\');
            }
            this.path = path.Substring(0, lastIndex);


            this.clients = new List<ProcessInfo>();
            this.transactionManagers = new List<ProcessInfo>();
            this.leaseManagers = new List<ProcessInfo>();
        }

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
                case "C":
                    this.clients.Add(new ProcessInfo(id, url));
                    break;
                case "T":
                    this.transactionManagers.Add(new ProcessInfo(id, url));
                    break;
                case "L":
                    this.leaseManagers.Add(new ProcessInfo(id, url));
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

        private void launchClient(ProcessInfo client)
        {
            this.Logger($"Creating new client with id '{client.getId()}' and url '{client.getUrl()}'");
            
            // TODO: add multiple console window launching
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                string command = "/c dotnet run --project " + this.path + $"\\Client\\Client.csproj {client.getId()} {client.getUrl()}";
                Process.Start(new ProcessStartInfo(@"cmd.exe ", @command) { UseShellExecute = true });
            }
            else
            {
                string command = $"run --project {path}/Client/Client.csproj {client.getId()} {client.getUrl()}";
                Process.Start("dotnet", command);
            }
        }

        private void launchTransactionManager(ProcessInfo transactionManager)
        {
            int clusterId = 0;
            string clusterNodes = "";

            for (int i = 0; i < this.transactionManagers.Count; i++)
            {
                if (this.transactionManagers[i] ==  transactionManager)
                {
                    clusterId = i;
                    continue;
                }
                clusterNodes += $"{i}-{this.transactionManagers[i].getId()}-{this.transactionManagers[i].getUrl()}";
            }

            this.Logger($"Creating new transaction manager with id '{transactionManager.getId()}' and url '{transactionManager.getUrl()}'");

            // TODO: add multiple console window launching
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                string command = "/c dotnet run --project " + this.path + $"\\TransactionManager\\TransactionManager.csproj {clusterId} {transactionManager.getId()} {transactionManager.getUrl()} {clusterNodes}";
                Process.Start(new ProcessStartInfo(@"cmd.exe ", @command) { UseShellExecute = true });
            }
            else
            {
                string command = $"run --project {path}/TransactionManager/TransactionManager.csproj {clusterId} {transactionManager.getId()} {transactionManager.getUrl()} {clusterNodes}";
                Process.Start("dotnet", command);

            }
        }

        private void launchLeaseManager(ProcessInfo leaseManager)
        {
            int clusterId = 0;
            string clusterNodes = "";

            string tmNodes = "";

            for (int i = 0; i < this.leaseManagers.Count; i++)
            {
                if (this.leaseManagers[i] == leaseManager)
                {
                    clusterId = i;
                    continue;
                }
                clusterNodes += $"{i}-{this.transactionManagers[i].getId()}-{this.transactionManagers[i].getUrl()}";
            }

            for (int i = 0; i < this.transactionManagers.Count; i++)
            {
                tmNodes += $"{i}-{this.transactionManagers[i].getId()}-{this.transactionManagers[i].getUrl()}";
            }

            this.Logger($"Creating new lease manager with id '{leaseManager.getId()}' and url '{leaseManager.getUrl()}'");

            // TODO: add multiple console window launching
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows)) 
            {
                string command = "/c dotnet run --project " + this.path + $"\\LeaseManager\\LeaseManager.csproj {clusterId} {leaseManager.getId()} {leaseManager.getUrl()} {clusterNodes} {tmNodes}";
                Process.Start(new ProcessStartInfo(@"cmd.exe ", @command) { UseShellExecute = true });
            }
            else 
            {
                string command = $"dotnet run --project {path}/LeaseManager/LeaseManager.csproj {clusterId} {leaseManager.getId()} {leaseManager.getUrl()} {clusterNodes} {tmNodes}";
                Process.Start("dotnet", command);
            }
        }

        public void launchProcesses()
        {
            foreach (ProcessInfo client in this.clients) {
                this.launchClient(client);
            }
            foreach (ProcessInfo transactionManager in this.transactionManagers)
            {
                this.launchTransactionManager(transactionManager);
            }
            foreach (ProcessInfo leaseManager in this.leaseManagers)
            {
                this.launchLeaseManager(leaseManager);
            }
        }
    }

    internal class ProcessInfo
    {
        private string id;
        private string url;

        public ProcessInfo(string id, string url)
        {
            this.id = id;
            this.url = url;
        }

        public string getId() { return this.id; }

        public string getUrl() { return this.url; }
    }
}
