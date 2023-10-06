namespace LeaseManager
{
    public class OtherLeaseManager
    {
        private List<Lease> GrantedLeases = new List<Lease>();
        private List<Lease> WaitingLeases = new List<Lease>();

        /// <summary>
        /// Adds a lease to the queue. If a lease owned by the
        /// same Transaction Manager already exists in the queue,
        /// simply adds new lease's keys which the existing one
        /// didn't contain already. Makes sure every conflict is
        /// adressed after.
        /// </summary>
        /// <param name="newLease">Lease to be added</param>
        public void AddLeaseToQueue(Lease newLease)
        {
            foreach (Lease existingLease in this.WaitingLeases)
            {
                if (existingLease.TmId == newLease.TmId) {
                    existingLease.AddKeys(newLease.Keys);
                    this.HandleLeaseBlockage(existingLease);
                    return;
                }
            }
            this.WaitingLeases.Add(newLease);
            this.HandleLeaseBlockage(newLease);
        }

        /// <summary>
        /// If lease conflicts with an already granted lease adds
        /// given lease to the conflict list of every conflicting 
        /// granted lease and the other way round
        /// </summary>
        /// <param name="lease">Potentially conflicting lease</param>
        private void HandleLeaseBlockage(Lease lease)
        {
            foreach (Lease granted in this.GrantedLeases)
            {
                if (lease.ConflictsWith(granted))
                {
                    granted.AddConflict(lease);
                    lease.AddConflict(granted);
                }
            }
        }
    }
}