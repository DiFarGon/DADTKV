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
            string url;

            if (type == "C")
            {
                url = tokens[3];
            }
            else
            {  // for now let's just run everything on the same machine
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

        // FIXME: sending the suspicions as a parameter to the processes might not be 
        // a good idea since there might be a lot of timeSlots and the string would be too big
        internal void handleF(string line)
        {
            string[] states = line.Split(' ');

            string timeSlot = states[1];

            int tmsN = 0;
            int lmsN = 0;
            bool found;
            Dictionary<ProcessInfo, string> nodesSuspicions = new Dictionary<ProcessInfo, string>();
            for (int i = 2; i < states.Length; i++)
            {
                if (tmsN < this.transactionManagers.Count)
                {
                    if (states[i] == "C" && transactionManagers[tmsN].getCrashTimeSlot() == "none")
                    {
                        transactionManagers[tmsN].setCrashTimeSlot(timeSlot);
                    }
                    tmsN++;
                }
                else if (lmsN < this.leaseManagers.Count)
                {
                    if (states[i] == "C" && leaseManagers[lmsN].getCrashTimeSlot() == "none")
                    {
                        leaseManagers[lmsN].setCrashTimeSlot(timeSlot);
                    }
                    lmsN++;
                }
                else
                {
                    found = false;
                    string[] sus = states[i].Trim('(', ')').Split(',');
                    foreach (ProcessInfo transactionManager in this.transactionManagers)
                    {
                        if (transactionManager.getId() == sus[0])
                        {
                            if (nodesSuspicions.ContainsKey(transactionManager))
                                nodesSuspicions[transactionManager] += $",{sus[1]}";
                            else
                                nodesSuspicions[transactionManager] = $"{timeSlot}:{sus[1]}";
                            found = true;
                            break;
                        }
                    }
                    if (!found)
                    {
                        foreach (ProcessInfo leaseManager in this.leaseManagers)
                        {
                            if (leaseManager.getId() == sus[0])
                            {
                                if (nodesSuspicions.ContainsKey(leaseManager))
                                    nodesSuspicions[leaseManager] += $",{sus[1]}";
                                else
                                    nodesSuspicions[leaseManager] = $"{timeSlot}:{sus[1]}";
                                break;
                            }
                        }
                    }
                }
            }
            foreach (KeyValuePair<ProcessInfo, string> nodeSuspicions in nodesSuspicions)
                nodeSuspicions.Key.addSuspicions($"{nodeSuspicions.Value}!");
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
            // <clusterId> <id> <url> <lms> <tms> <time_slots> <start_time> <time_slot_duration> <crash_time_slot> <failure_suspicions> <debug?>

            this.Logger($"Creating new transaction manager with id '{transactionManager.getId()}' and url '{transactionManager.getUrl()}'");

            (int clusterId, string clusterNodes) = this.getClusterIdAndTransactionManagersString(transactionManager);
            string arguments = $"{clusterId} {transactionManager.getId()} {transactionManager.getUrl()} {this.getAllLeaseManagersString()} {clusterNodes} {this.slots} {this.startTime} {this.slotDuration} {transactionManager.getCrashTimeSlot()} {transactionManager.getSuspicions()}";
            if (this.debug) { arguments += " debug"; }

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                string command = $"/c dotnet run --project {this.path}\\TransactionManager\\TransactionManager.csproj {arguments}";
                Process.Start(new ProcessStartInfo(@"cmd.exe ", @command) { UseShellExecute = true });
            }
            // TODO: add multiple console window launching
            else
            {
                string command = $"dotnet run --project {path}/TransactionManager/TransactionManager.csproj {arguments}";
                Process.Start("gnome-terminal", $"-- bash -c \"{command}; exec bash\"");

            }
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

        private void launchLeaseManager(ProcessInfo leaseManager)
        {
            // <clusterId> <id> <url> <lms> <tms> <time_slots> <start_time> <time_slot_duration> <crash_time_slot> <failure_suspicions> <debug?>

            this.Logger($"Creating new lease manager with id '{leaseManager.getId()}' and url '{leaseManager.getUrl()}'");

            (int clusterId, string clusterNodes) = this.getClusterIdAndLeaseManagersString(leaseManager);

            string arguments = $"{clusterId} {leaseManager.getId()} {leaseManager.getUrl()} {clusterNodes} {this.getAllTransactionManagersString()} {this.slots} {this.startTime} {this.slotDuration} {leaseManager.getCrashTimeSlot()} {leaseManager.getSuspicions()}";
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
                Process.Start("gnome-terminal", $"-- bash -c \"{command}; exec bash\"");
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

        public void launchProcesses()
        {
            foreach (ProcessInfo client in this.clients)
            {
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
        private string crashTimeSlot = "none";
        private string suspicions = "none"; // format: <time_slot_1>:<suspected_node_id_1>,<suspected_node_id_2>!<time_slot_2>:<suspected_node_id_1>!

        public ProcessInfo(string id, string url)
        {
            this.id = id;
            this.url = url;
        }

        public string getId() { return this.id; }

        public string getUrl() { return this.url; }

        public string getCrashTimeSlot() { return this.crashTimeSlot; }

        public void setCrashTimeSlot(string crashTimeSlot)
        {
            this.crashTimeSlot = crashTimeSlot;
        }

        public string getSuspicions() { return this.suspicions; }

        public void addSuspicions(string suspicions) // format: <time_slot_1>:<suspected_node_id_1>,<suspected_node_id_2>!
        {
            if (this.suspicions == "none")
                this.suspicions = "";
            this.suspicions += suspicions;
        }
    }
}
