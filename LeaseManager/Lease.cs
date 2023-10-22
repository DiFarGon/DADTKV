namespace LeaseManager
{
    public class Lease
    {
        public readonly string TmId;
        public readonly List<string> Keys;

        /// <summary>
        /// Creates a new lease
        /// </summary>
        /// <param name="tmId">ID of TransactionManager who owns the lease</param>
        /// <param name="keys">List of keys held by lease</param>
        public Lease(string tmId, List<string> keys)
        {
            this.TmId = tmId;
            this.Keys = keys;
        }

        /// <summary>
        /// Adds every given key to the lease unless it's already there
        /// </summary>
        /// <param name="keys"> List of keys to add </param>
        public void AddKeys(List<String> keys)
        {
            foreach (string key in keys)
            {
                if (!this.Keys.Contains(key))
                {
                    this.Keys.Add(key);
                }
            }
        }

        public override bool Equals(object? obj)
        {
            return obj is Lease lease &&
                   TmId == lease.TmId &&
                   EqualityComparer<List<string>>.Default.Equals(Keys, lease.Keys);
        }
    }
}