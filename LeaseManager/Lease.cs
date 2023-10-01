namespace LeaseManager
{
    public class Lease
    {
        private string tmId;
        List<string> dataKeys;

        public Lease(string tmId, List<string> dataKeys)
        {
            this.tmId = tmId;
            this.dataKeys = dataKeys;
        }

        public string getTmId()
        {
            return this.tmId;
        }

        public List<string> getDataKeys()
        {
            return this.dataKeys;
        }

        public bool conflictsWith(Lease other)
        {
            foreach (string key in this.dataKeys)
            {
                if (other.dataKeys.Contains(key))
                    return true;
            }
            return false;
        }

        public bool isSame(string tmId, List<string> dataKeys)
        {
            if (this.tmId == tmId && this.dataKeys == dataKeys)
                return true;
            else
                return false;
        }
    }
}