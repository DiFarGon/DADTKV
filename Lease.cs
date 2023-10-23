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
            if (this.TmId == lease.TmId) return false;
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

        /// <summary>
        /// Checks if this lease is equal to the given object
        /// </summary>
        /// <param name="obj"></param>
        /// <returns>true if lease is equal, false otherwise</returns>
        public override bool Equals(object? obj)
        {
            return obj is Lease lease &&
                   TmId == lease.TmId &&
                   EqualityComparer<List<string>>.Default.Equals(Keys, lease.Keys);
        }

        /// <summary>
        /// Checks if two leases are equal
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <returns>true if right is equal to left, false otherwise</returns>
        public static bool operator ==(Lease left, Lease right)
        {
            if (ReferenceEquals(left, right)) return true;
            if (left is null || right is null) return true;
            return left.TmId == right.TmId &&
                   EqualityComparer<List<string>>.Default.Equals(left.Keys, right.Keys);
        }

        /// <summary>
        /// Checks if two leases aren't equal
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <returns>true if leases are different, false if leases are equal</returns>
        public static bool operator !=(Lease left, Lease right)
        {
            return !(left == right);
        }
    }
}