using System.Security.Cryptography.X509Certificates;

namespace dadtkv
{
    internal class Program
    {
        public static void Main(string[] args)
        {
            if (args.Length != 1 && args.Length != 2)
            {
                Console.Error.WriteLine("Wrong number of arguments!");
                return;
            }

            bool debug = false;
            if (args.Length == 2 && args[1] == "debug") {
                debug = true;
            }

            MainProcess mainProcess = new MainProcess(debug);

            Console.WriteLine(args[0]);
            string[] script = File.ReadAllLines(args[0]);

            foreach (string line in script)
            {
                switch (line[0])
                {
                    case '#':
                        continue;
                    case 'P':
                        mainProcess.handleP(line);
                        break;
                    case 'S':
                        mainProcess.handleS(line);
                        break;
                    case 'D':
                        mainProcess.handleD(line);
                        break;
                    case 'T':
                        mainProcess.handleT(line);
                        break;
                    case 'F':
                        mainProcess.handleF(line);
                        break;
                }
            }
            mainProcess.launchProcesses();
            return;
        }
    }
}