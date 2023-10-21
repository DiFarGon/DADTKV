using System.Diagnostics;
using System.Transactions;
using System.Runtime.InteropServices;
using System.Globalization;

namespace dadtkv
{
    internal class MainProcess
    {
        private bool debug;
        private string path;
        private List<ProcessInfo> clients;
        private List<ProcessInfo> transactionManagers;
        private List<ProcessInfo> leaseManagers;
        private int slots;
        private int slotDuration;
        private string startTime = "";
        private Func<ProcessInfo> assignTransactionManager;

        public MainProcess(bool debug)
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

            int next = 0;

            this.assignTransactionManager = () => {
                ProcessInfo transactionManager = this.transactionManagers[next];
                if (next == this.transactionManagers.Count - 1)
                {
                    next = 0;
                }
                else
                {
                    next += 1;
                }
                return transactionManager;
            };
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
            string url;

            if (type == "C")
            {
                url = tokens[3];
            }
            else {  // for now let's just run everything on the same machine
                int port = new Uri(tokens[3]).Port;
                url = $"http://localhost:{port}";
            }

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
            this.slots = int.Parse(line.Split(' ')[1]);

            this.Logger($"Test lasts {this.slots} slots");
        }

        internal void handleD(string line)
        {
            this.slotDuration = int.Parse(line.Split(' ')[1]);

            this.Logger($"Each slot lasts {this.slotDuration} miliseconds");
        }

        internal void handleT(string line)
        {
            this.startTime = line.Split(' ')[1];
            this.Logger($"Test starts at {this.startTime}");
        }

        internal void handleF(string line)
        {
            this.Logger("I don't really want to do this right now");
        }

        private void launchClient(ProcessInfo client)
        {
            // <id> <url> <port>? <id> <tms> <startTime> <debug?>

            this.Logger($"Creating new client with id '{client.getId()}' and url '{client.getUrl()}'");
            
            string arguments = $"{client.getId()} {client.getUrl()} {this.assignTransactionManager().getId()} {this.getAllTransactionManagersString()} {this.startTime}";
            if (this.debug) { arguments += " debug"; }

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                string command = $"/c dotnet run --project {this.path}\\Client\\Client.csproj {arguments}";
                Process.Start(new ProcessStartInfo(@"cmd.exe ", @command) { UseShellExecute = true });
            }
            // TODO: add multiple console window launching
            else
            {
                string command = $"run --project {path}/Client/Client.csproj {arguments}";
                Process.Start("dotnet", command);
            }
        }

        private void launchTransactionManager(ProcessInfo transactionManager)
        {
            // <clusterId> <id> <url> <lms> <tms> <debug?>

            this.Logger($"Creating new transaction manager with id '{transactionManager.getId()}' and url '{transactionManager.getUrl()}'");

            (int clusterId, string clusterNodes) = this.getClusterIdAndTransactionManagersString(transactionManager);
            string arguments = $"{clusterId} {transactionManager.getId()} {transactionManager.getUrl()} {this.getAllLeaseManagersString()} {clusterNodes}";
            if (this.debug) { arguments += " debug"; }

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                string command = $"/c dotnet run --project {this.path}\\TransactionManager\\TransactionManager.csproj {arguments}";
                Process.Start(new ProcessStartInfo(@"cmd.exe ", @command) { UseShellExecute = true });
            }
            // TODO: add multiple console window launching
            else
            {
                string command = $"run --project {path}/TransactionManager/TransactionManager.csproj {arguments}";
                Process.Start("dotnet", command);

            }
        }

        private string getAllLeaseManagersString()
        {
            string leaseManagers = "";
            for (int i = 0; i < this.leaseManagers.Count; i++)
            {
                leaseManagers += $"{i}-{this.leaseManagers[i].getId()}-{this.leaseManagers[i].getUrl()};";
            }
            return leaseManagers;  
        }

        private (int, string) getClusterIdAndTransactionManagersString(ProcessInfo transactionManager)
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
                clusterNodes += $"{i}-{this.transactionManagers[i].getId()}-{this.transactionManagers[i].getUrl()};";
            }

            return (clusterId, clusterNodes);
        }

        private void launchLeaseManager(ProcessInfo leaseManager)
        {
            // <clusterId> <id> <url> <port> <lms> <tms> <duration> <startTime> <debug?>
            
            this.Logger($"Creating new lease manager with id '{leaseManager.getId()}' and url '{leaseManager.getUrl()}'");

            (int clusterId, string clusterNodes) = this.getClusterIdAndLeaseManagersString(leaseManager);
            string port = leaseManager.getUrl().Split(':')[2];

            string arguments = $"{clusterId} {leaseManager.getId()} {leaseManager.getUrl()} {port} {clusterNodes} {this.getAllTransactionManagersString()} {this.slotDuration} {this.startTime}";
            if (this.debug) { arguments += " debug"; }
            
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows)) 
            {
                string command = $"/c dotnet run --project {this.path}\\LeaseManager\\LeaseManager.csproj {arguments}";
                Process.Start(new ProcessStartInfo(@"cmd.exe ", @command) { UseShellExecute = true });
            }
            // TODO: add multiple console window launching
            else 
            {
                string command = $"dotnet run --project {this.path}/LeaseManager/LeaseManager.csproj {arguments}";
                Process.Start("dotnet", command);
            }
        }

        private (int, string) getClusterIdAndLeaseManagersString(ProcessInfo leaseManager)
        {
            int clusterId = 0;
            string clusterNodes = "";

            for (int i = 0; i < this.leaseManagers.Count; i++)
            {
                if (this.leaseManagers[i] == leaseManager)
                {
                    clusterId = i;
                    continue;
                }
                clusterNodes += $"{i}-{this.leaseManagers[i].getId()}-{this.leaseManagers[i].getUrl()};";
            }

            return (clusterId, clusterNodes);
        }
        
        private string getAllTransactionManagersString()
        {
            string transactionManagers = "";
            for (int i = 0; i < this.transactionManagers.Count; i++)
            {
                transactionManagers += $"{i}-{this.transactionManagers[i].getId()}-{this.transactionManagers[i].getUrl()};";
            }
            return transactionManagers;
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
