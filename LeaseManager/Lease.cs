namespace LeaseManager
{
    public class Lease {
        public readonly string TmId;
        public readonly List<string> Keys;
        public readonly List<Lease> ConflictList = new List<Lease>();

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
        /// Adds a lease to the conflict list
        /// </summary>
        /// <param name="lease">Lease to add</param>
        public void AddConflict(Lease lease)
        {
            this.ConflictList.Add(lease);
        }

        /// <summary>
        /// Removes a lease to the conflict list
        /// </summary>
        /// <param name="lease">Lease to remove</param>
        public void RemoveConflict(Lease lease)
        {
            this.ConflictList.Remove(lease);
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

        /// <summary>
        /// Checks if this lease conflicts with the given one
        /// </summary>
        /// <param name="lease">Potentially conflictig lease</param>
        /// <returns></returns>
        public bool ConflictsWith(Lease lease)
        {
            foreach (string key in lease.Keys)
            {
                if (this.Keys.Contains(key))
                {
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Checks which keys make leases conflict returning a list.
        /// If no keys conflict, returns an empty list
        /// </summary>
        /// <param name="lease"></param>
        /// <returns>List of conflicting keys</returns>
        public List<string> ConflictingKeys(Lease lease) {
            List<string> conflictingKeys = new List<String>();
            if (!this.ConflictList.Contains(lease)) { return conflictingKeys; }
            foreach (string key in lease.Keys)
            {
                if (this.Keys.Contains(key)) { conflictingKeys.Add(key); }
            }
            return conflictingKeys;
        }

        /// <summary>
        /// Checks if lease has any conflicts
        /// </summary>
        /// <returns>true if lease has conflicts,
        /// false otherwise</returns>
        public bool HasConflicts() { return this.ConflictList.Count == 0; }
    }
}