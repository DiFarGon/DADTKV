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

        /// <summary>
        /// Creates a bew Transaction instance from a TransactionMessage
        /// </summary>
        /// <param name="transactionMessage"></param>
        public Transaction(TransactionMessage transactionMessage)
        {
            this.ReadKeys = new List<string>(transactionMessage.KeysRead);

            this.DadIntsWrite = new List<DadInt.DadInt>();
            transactionMessage.DadIntsWrite.ToList().ForEach(dadInt => {
                this.DadIntsWrite.Add(new DadInt.DadInt(dadInt.Key, dadInt.Value));
            });
        }

        /// <summary>
        /// Creates a TransactionMessage from this Transaction instance
        /// </summary>
        /// <returns></returns>
        public TransactionMessage ToTransactionMessage()
        {
            TransactionMessage transactionMessage = new TransactionMessage();
            foreach (DadInt.DadInt dadInt in this.DadIntsWrite)
            {
                transactionMessage.DadIntsWrite.Add(new DadIntMessage
                    {
                        Key = dadInt.Key,
                        Value = dadInt.Value
                    });
            }
            foreach (string key in this.ReadKeys)
            {
                transactionMessage.KeysRead.Add(key);
            }
            return transactionMessage;
        }
    }
}