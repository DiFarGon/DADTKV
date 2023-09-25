namespace Client
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

            Client client = new Client(args[0], args[1]);
        }
    }
}