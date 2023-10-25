using System.Diagnostics;
using System.Transactions;
using System.Runtime.InteropServices;
using System.Globalization;
using System.IO.Pipes;

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
        private string testFolder;

        private List<int> generatedProcessesIds = new List<int>();

        public MainProcess(string testFolder, bool debug)
        {
            this.testFolder = testFolder;
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

            this.assignTransactionManager = () =>
            {
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

            string url = tokens[3];

            // if (type != "C")
            // {  // for now let's just run everything on the same machine
            //     int port = new Uri(tokens[3]).Port;
            //     url = $"http://localhost:{port}";
            // }

            switch (type)
            {
                case "C":
                    ProcessInfo p = new ProcessInfo(id);
                    p.setClientScriptFile("../dadtkv/" + tokens[3]);
                    this.clients.Add(p);
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

        private int launchClient(ProcessInfo client)
        {
            // <id> <tms> <tm_id> <startTime> <client_script_file> <debug?>

            this.Logger($"Creating new client with id '{client.getId()}' and url '{client.getUrl()}'");

            int pid;

            string arguments = $"{client.getId()} {this.getAllTransactionManagersString()} {this.assignTransactionManager().getId()} {this.startTime} {client.getClientScriptFile()} ";
            if (this.debug) { arguments += " debug"; }

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                string command = $"/c dotnet run --project {this.path}\\Client\\Client.csproj {arguments}";
                Process.Start(new ProcessStartInfo(@"cmd.exe ", @command) { UseShellExecute = true });
                pid = 0;
            }
            else
            {
                string command = $"dotnet run --project {path}/Client/Client.csproj {arguments}";
                ProcessStartInfo psi = new ProcessStartInfo("gnome-terminal")
                {
                    Arguments = $"-- bash -c \"{command}; exec bash\"",
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    CreateNoWindow = true
                };
                Process process = new Process { StartInfo = psi };
                process.Start();
                pid = process.Id;
            }
            return pid;
        }

        private int launchTransactionManager(ProcessInfo transactionManager)
        {
            // <clusterId> <id> <url> <lms> <tms> <time_slots> <start_time> <time_slot_duration> <config_file> <debug?>

            this.Logger($"Creating new transaction manager with id '{transactionManager.getId()}' and url '{transactionManager.getUrl()}'");

            int pid;

            (int clusterId, string clusterNodes) = this.getClusterIdAndTransactionManagersString(transactionManager);
            if (clusterNodes == "") clusterNodes = "!"; // some ugly hammering to make the code work

            string arguments = $"{clusterId} {transactionManager.getId()} {transactionManager.getUrl()} {this.getAllLeaseManagersString()} {clusterNodes} {this.slots} {this.startTime} {this.slotDuration} {getConfigFile()}";
            if (this.debug) { arguments += " debug"; }

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                string command = $"/c dotnet run --project {this.path}\\TransactionManager\\TransactionManager.csproj {arguments}";
                Process.Start(new ProcessStartInfo(@"cmd.exe ", @command) { UseShellExecute = true });
                pid = 0;
            }
            else
            {
                string command = $"dotnet run --project {path}/TransactionManager/TransactionManager.csproj {arguments}";
                ProcessStartInfo psi = new ProcessStartInfo("gnome-terminal")
                {
                    Arguments = $"-- bash -c \"{command}; exec bash\"",
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    CreateNoWindow = true
                };
                Process process = new Process { StartInfo = psi };
                process.Start();
                pid = process.Id;
            }
            return pid;
        }

        private string getAllLeaseManagersString()
        {
            string leaseManagers = "";
            for (int i = 0; i < this.leaseManagers.Count; i++)
            {
                leaseManagers += $"{i}-{this.leaseManagers[i].getId()}-{this.leaseManagers[i].getUrl()}!";
            }
            return leaseManagers;
        }

        private (int, string) getClusterIdAndTransactionManagersString(ProcessInfo transactionManager)
        {
            int clusterId = 0;
            string clusterNodes = "";

            for (int i = 0; i < this.transactionManagers.Count; i++)
            {
                if (this.transactionManagers[i] == transactionManager)
                {
                    clusterId = i;
                    continue;
                }
                clusterNodes += $"{i}-{this.transactionManagers[i].getId()}-{this.transactionManagers[i].getUrl()}!";
            }

            return (clusterId, clusterNodes);
        }

        private int launchLeaseManager(ProcessInfo leaseManager)
        {
            // <clusterId> <id> <url> <lms> <tms> <time_slots> <start_time> <time_slot_duration> <config_file> <debug?>

            this.Logger($"Creating new lease manager with id '{leaseManager.getId()}' and url '{leaseManager.getUrl()}'");

            int pid;

            (int clusterId, string clusterNodes) = this.getClusterIdAndLeaseManagersString(leaseManager);

            if (clusterNodes == "") clusterNodes = "!"; // some ugly hammering to make the code work

            string arguments = $"{clusterId} {leaseManager.getId()} {leaseManager.getUrl()} {clusterNodes} {this.getAllTransactionManagersString()} {this.slots} {this.startTime} {this.slotDuration} {getConfigFile()}";
            if (this.debug) { arguments += " debug"; }

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                string command = $"/c dotnet run --project {this.path}\\LeaseManager\\LeaseManager.csproj {arguments}";
                Process.Start(new ProcessStartInfo(@"cmd.exe ", @command) { UseShellExecute = true });
                pid = 0;
            }
            else
            {
                string command = $"dotnet run --project {this.path}/LeaseManager/LeaseManager.csproj {arguments}";
                ProcessStartInfo psi = new ProcessStartInfo("gnome-terminal")
                {
                    Arguments = $"-- bash -c \"{command}; exec bash\"",
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    CreateNoWindow = true
                };
                Process process = new Process { StartInfo = psi };
                process.Start();
                pid = process.Id;
            }
            return pid;
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
                clusterNodes += $"{i}-{this.leaseManagers[i].getId()}-{this.leaseManagers[i].getUrl()}!";
            }

            return (clusterId, clusterNodes);
        }

        private string getAllTransactionManagersString()
        {
            string transactionManagers = "";
            for (int i = 0; i < this.transactionManagers.Count; i++)
            {
                transactionManagers += $"{i}-{this.transactionManagers[i].getId()}-{this.transactionManagers[i].getUrl()}!";
            }
            return transactionManagers;
        }

        public string getConfigFile() { return this.testFolder + "/config.txt"; }

        public void launchProcesses()
        {
            foreach (ProcessInfo transactionManager in this.transactionManagers)
            {
                generatedProcessesIds.Add(launchTransactionManager(transactionManager));
            }
            foreach (ProcessInfo leaseManager in this.leaseManagers)
            {
                generatedProcessesIds.Add(launchLeaseManager(leaseManager));
            }
            foreach (ProcessInfo client in this.clients)
            {
                generatedProcessesIds.Add(launchClient(client));
            }
        }

        public void terminateProcesses()
        {
            foreach (int pid in generatedProcessesIds)
            {
                Process process = Process.GetProcessById(pid);
                if (!process.HasExited)
                    process.Kill(); // TODO: this should be a signal so that the processes can gracefully terminate (close channels and such)
            }
        }
    }

    internal class ProcessInfo
    {
        private string id;
        private string url = "";
        private string clientSrciptFile = "";

        public ProcessInfo(string id, string url)
        {
            this.id = id;
            this.url = url;
        }

        public ProcessInfo(string id)
        {
            this.id = id;
        }

        public string getId() { return this.id; }

        public string getUrl() { return this.url; }

        public string getClientScriptFile() { return this.clientSrciptFile; }
        public void setClientScriptFile(string clientScriptFile) { this.clientSrciptFile = clientScriptFile; }
    }
}
