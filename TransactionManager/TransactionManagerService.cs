using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Lease;

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
            this.transactionManager.Logger("Received Transaction Request");

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
        public override Task<AcknowledgeConsensusResponse> AcknowledgeConsensus(AcknowledgeConsensusRequest request, ServerCallContext context)
        {
            this.transactionManager.Logger("Acnkowledged consensus");

            List<Lease.Lease> leases = new List<Lease.Lease>();
            request.Leases.ToList().ForEach(lease => {
                leases.Add(new Lease.Lease(lease.TmId, new List<string>(lease.Keys)));
            });
            this.transactionManager.SetCurrentLeases(leases);
            this.transactionManager.AttemptFirstTransaction();
            AcknowledgeConsensusResponse response = new AcknowledgeConsensusResponse();
            return Task.FromResult(response);
        }

        public override Task<TransactionExecutedResponse> TransactionExecuted(TransactionExecutedRequest request, ServerCallContext context)
        {
            this.transactionManager.Logger("Acknowledged execution of transaction");

            Transaction.Transaction transaction = new Transaction.Transaction(request.TransactionMessage);
            this.transactionManager.WriteTransactionToStore(transaction);
            TransactionExecutedResponse response = new TransactionExecutedResponse();
            return Task.FromResult(response);
        }
    }
}
