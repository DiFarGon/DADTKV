namespace LeaseManager
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
        /// Adds the given key to this lease's keys list
        /// </summary>
        /// <param name="key"></param>
        public void AddKey(string key)
        {
            this.Keys.Add(key);
        }
    }
}