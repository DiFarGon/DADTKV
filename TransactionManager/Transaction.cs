namespace Transaction
{
    public class Transaction {
        public readonly List<string> ReadKeys;
        public readonly List<DadInt.DadInt> DadIntsWrite;

        /// <summary>
        /// Creates a new Transaction instance
        /// </summary>
        /// <param name="readKeys"></param>
        /// <param name="dadIntsWrite"></param>
        public Transaction(List<string> readKeys, List<DadInt.DadInt> dadIntsWrite)
        {
            this.ReadKeys = readKeys;
            this.DadIntsWrite = dadIntsWrite;
        }

        public Transaction(TransactionMessage transactionMessage)
        {
            this.ReadKeys = new List<string>(transactionMessage.KeysRead);

            this.DadIntsWrite = new List<DadInt.DadInt>();
            transactionMessage.DadIntsWrite.ToList().ForEach(dadInt => {
                this.DadIntsWrite.Add(new DadInt.DadInt(dadInt.Key, dadInt.Value));
            });
        }
    }
}