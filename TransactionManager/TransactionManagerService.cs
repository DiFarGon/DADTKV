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

        public override Task<TransactionResponse> Transaction(TransactionRequest transactionRequest, ServerCallContext context)
        {
            transactionManager.Logger("Received Transaction Request");

            LeaseRequest leaseRequest = new LeaseRequest { TmId = transactionManager.GetId() };

            List<string> keysRead = new List<string>(transactionRequest.KeysRead);
            List<DadInt> dadIntsWrite = new List<DadInt>(transactionRequest.KeysWrite);
            
            List<string> keysWrite = new List<string>();
            foreach (DadInt dadInt in dadIntsWrite)
            {
                keysWrite.Add(dadInt.Key);
            }

            List<string> keys = keysRead.Concat(keysWrite).ToList();

            foreach (string key in keys)
            {
                leaseRequest.Keys.Add(key);
            }

            transactionManager.Logger("Broadcasting lease request to lease managers");

            foreach (int clusterId in transactionManager.GetLeaseManagersServices().Keys)
            {
                LeaseManagerService.LeaseManagerServiceClient channel = transactionManager.GetLeaseManagersServices()[clusterId].Item2;
                channel.Lease(leaseRequest);
            }

            var response = new TransactionResponse();
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
