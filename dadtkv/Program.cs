namespace dadtkv
{
    internal class Program
    {
        public static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                Console.Error.WriteLine("Not enough arguments!");
                return;
            }

            string[] script = File.ReadAllLines(args[0]);

            MainProcess mainProcess = new MainProcess();

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
            return;
        }
    }
}