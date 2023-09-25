namespace LeaseManager
{
    internal class Program
    {
        public static void Main(string[] args)
        {
            if (args.Length != 2)
            {
                Console.Error.WriteLine("wrong arguments!");
                return;
            }

            LeaseManager leaseManager = new LeaseManager(args[0], args[1]);
        }
    }
}