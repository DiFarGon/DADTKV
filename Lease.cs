namespace Lease
{
    /// <summary>
    /// Represents a Lease by containing the respective Transaction Manager id
    /// and a list of keys this Lease holds
    /// </summary>
    public class Lease
    {
        public readonly string TmId;
        public readonly List<string> Keys;

        /// <summary>
        /// Creates a Lease with the given Transaction Manager id and list of keys
        /// </summary>
        /// <param name="tmId"></param>
        /// <param name="keys"></param>
        public Lease(string tmId, List<string> keys)
        {
            this.TmId = tmId;
            this.Keys = keys;
        }

        /// <summary>
        /// Creates a Lease with the given Transaction Manager id and
        /// an empty list of keys
        /// </summary>
        /// <param name="tmId"></param>
        public Lease(string tmId)
        {
            this.TmId = tmId;
            this.Keys = new List<string>();
        }

        /// <summary>
        /// Adds the given key to this Lease's keys list
        /// </summary>
        /// <param name="key"></param>
        public void AddKey(string key)
        {
            this.Keys.Add(key);
        }

        /// <summary>
        /// Checks if this Lease conflicts with the given Lease
        /// </summary>
        /// <param name="lease"></param>
        /// <returns>true if Leases conflict, false otherwise</returns>
        public bool ConflictsWith(Lease lease)
        {
            foreach(string key in lease.Keys)
            {
                if (this.Keys.Contains(key)) { return true; }
            }
            return true;
        }

        /// <summary>
        /// Checks if this Lease conflicts with any lease of the given lease
        /// </summary>
        /// <param name="leases"></param>
        /// <returns>true if there's a conflicting lease, false otherwise</returns>
        public bool ConflictsWithAny(List<Lease> leases)
        {
            foreach (Lease lease in leases)
            {
                if (this.ConflictsWith(lease)) { return true; }
            }
            return false;
        }
    }
}