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
        public override Task<TransactionResponse> Transaction(TransactionRequest transactionRequest, ServerCallContext context)
        {
            this.transactionManager.Logger("Received Transaction Request");

            LeaseRequest leaseRequest = new LeaseRequest { TmId = transactionManager.Id };

            List<string> keysRead = new List<string>(transactionRequest.KeysRead);

            List<DadInt.DadInt> dadIntsWrite = new List<DadInt.DadInt>();
            transactionRequest.DadIntsWrite.ToList().ForEach(dadInt => {
                dadIntsWrite.Add(new DadInt.DadInt(dadInt.Key, dadInt.Value));
            });

            var response = new TransactionResponse();

            if (this.transactionManager.ExecuteTransaction(keysRead, dadIntsWrite))
            {
                return Task.FromResult(response);
            }
            
            List<string> keysWrite = new List<string>();
            foreach (DadInt.DadInt dadInt in dadIntsWrite)
            {
                keysWrite.Add(dadInt.Key);
            }

            List<string> keys = keysRead.Concat(keysWrite).ToList();

            foreach (string key in keys)
            {
                leaseRequest.Keys.Add(key);
            }

            transactionManager.Logger("Broadcasting lease request to lease managers");

            foreach (LeaseManagerService.LeaseManagerServiceClient leaseManagerService in transactionManager.LmServices.Values)
            {
                LeaseManagerService.LeaseManagerServiceClient channel = leaseManagerService;
                channel.Lease(leaseRequest);
            }

            return Task.FromResult(response);
        }

        public override Task<StatusResponse> Status(StatusRequest statusRequest, ServerCallContext context)
        {
            transactionManager.Logger("I'm Alive");
            var reply = new StatusResponse();
            return Task.FromResult(reply);
        }

        public override Task<AcknowledgeConsensusResponse> AcknowledgeConsensus(AcknowledgeConsensusRequest request, ServerCallContext context)
        {
            AcknowledgeConsensusResponse response = new AcknowledgeConsensusResponse();
            return Task.FromResult(response);
        }
    }
}
