using Grpc.Core;

namespace TransactionManager
{
    class TransactionManagerServiceImpl : TransactionManagerService.TransactionManagerServiceBase
    {
        TransactionManager transactionManager;

        /// <summary>
        /// Creates a new instance of TransactionManagerServiceImpl
        /// </summary>
        /// <param name="transactionManager"></param>
        public TransactionManagerServiceImpl(TransactionManager transactionManager)
        {
            this.transactionManager = transactionManager;
        }

        /// <summary>
        /// On receiving a Transaction request from a Client tries to execute
        /// the transaction. If it fails broadcasts a Lease request to all
        /// Lease Managers.
        /// </summary>
        /// <param name="transactionRequest"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async override Task<TransactionResponse> Transaction(TransactionRequest transactionRequest, ServerCallContext context)
        {
            this.transactionManager.Logger("Received Transaction Request: " + transactionRequest.TransactionMessage.KeysRead + "\n");

            TaskCompletionSource<TransactionResponse> tcs = new TaskCompletionSource<TransactionResponse>();

            Transaction.Transaction transaction = new Transaction.Transaction(transactionRequest.TransactionMessage);

            (bool success, List<DadInt.DadInt> read) = this.transactionManager.AttemptTransaction(transaction);
            if (success)
            {
                List<DadIntMessage> dadIntMessages = new List<DadIntMessage>();
                foreach (DadInt.DadInt dadInt in read)
                {
                    dadIntMessages.Add(new DadIntMessage
                    {
                        Key = dadInt.Key,
                        Value = dadInt.Value
                    });
                }
                TransactionResponse response = new TransactionResponse();
                response.Read.AddRange(dadIntMessages);
                tcs.SetResult(response);
                return tcs.Task.Result;
            }

            this.transactionManager.RequestLease(transaction);

            this.transactionManager.StageTransaction(transaction, tcs);

            await tcs.Task;

            return tcs.Task.Result;
        }

        /// <summary>
        /// Handles a Status rpc call, by logging this Transaction Manager status
        /// </summary>
        /// <param name="statusRequest"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<StatusResponse> Status(StatusRequest statusRequest, ServerCallContext context)
        {
            transactionManager.Logger("I'm Alive");

            var reply = new StatusResponse();
            return Task.FromResult(reply);
        }

        /// <summary>
        /// Handles an AcknowledgeConsensus rpc by making
        /// this Transaction Manager know what current leases
        /// were assigned by the system
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<InstanceResultResponse> InstanceResult(InstanceResultRequest request, ServerCallContext context)
        {
            this.transactionManager.Logger($"Acnkowledged consensus on instance {request.InstanceId}.");

            List<Lease.Lease> leases = new List<Lease.Lease>();
            request.Result.ToList().ForEach(lease =>
            {
                leases.Add(new Lease.Lease(lease.TmId, new List<string>(lease.DataKeys)));
            });
            this.transactionManager.SetCurrentLeases(leases);
            InstanceResultResponse response = new InstanceResultResponse();
            return Task.FromResult(response);
        }

        /// <summary>
        /// Handles a TransactionExecutedRequest by writing the changes
        /// made to the DadInts store
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<TransactionExecutedResponse> TransactionExecuted(TransactionExecutedRequest request, ServerCallContext context)
        {
            this.transactionManager.Logger("Acknowledged execution of transaction");

            Transaction.Transaction transaction = new Transaction.Transaction(request.TransactionMessage);
            this.transactionManager.WriteTransactionToStore(transaction);
            TransactionExecutedResponse response = new TransactionExecutedResponse();
            return Task.FromResult(response);
        }

        public override Task<LeaseReleasedResponse> LeaseReleased(LeaseReleasedRequest request, ServerCallContext context)
        {
            this.transactionManager.Logger("Acknowledged releasing of lease");

            Lease.Lease lease = new Lease.Lease(request.LeaseMessage);
            this.transactionManager.RemoveLease(lease);

            LeaseReleasedResponse response = new LeaseReleasedResponse();
            return Task.FromResult(response);
        }

        public override Task<ProposeTransactionResponse> ProposeTransaction(ProposeTransactionRequest request, ServerCallContext context)
        {
            ProposeTransactionResponse response = new ProposeTransactionResponse { Accept = !this.transactionManager.Suspects(request.TmId) };
            return Task.FromResult(response);
        }
    }
}
