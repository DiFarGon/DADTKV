namespace TransactionManager
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
            TransactionManager transactionManager = new TransactionManager(args[0], args[1]);
        }
    }
}