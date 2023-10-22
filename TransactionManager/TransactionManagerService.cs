using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;

namespace TransactionManager
{
    class TransactionManagerServiceImpl : ClientService.ClientServiceBase
    {

        TransactionManager transactionManager;
        public TransactionManagerServiceImpl(TransactionManager transactionManager)
        {
            this.transactionManager = transactionManager;
        }

        public override Task<TransactionReply> Transaction(TransactionRequest transactionRequest, ServerCallContext context)
        {
            transactionManager.Logger("Received Transaction Request");

            LeaseRequest leaseRequest = new LeaseRequest { TmId = transactionManager.getId() };

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

            foreach (int clusterId in transactionManager.getLeaseManagersServices().Keys)
            {
                LeaseManagerService.LeaseManagerServiceClient channel = transactionManager.getLeaseManagersServices()[clusterId].Item2;
                channel.LeaseAsync(leaseRequest);
            }

            var reply = new TransactionReply();
            return Task.FromResult(reply);
        }

        public override Task<StatusReply> Status(StatusRequest statusRequest, ServerCallContext context)
        {
            Console.WriteLine("I'm Alive");
            transactionManager.sendStatusRequests();
            var reply = new StatusReply();
            return Task.FromResult(reply);
        }


    }
    class TransactionManagerServiceImpl_TM : TransactionManagerService.TransactionManagerServiceBase
    {
        TransactionManager transactionManager;
        public TransactionManagerServiceImpl_TM(TransactionManager transactionManager)
        {
            this.transactionManager = transactionManager;
        }

        public override Task<StatusReply_TM> Status_TM(StatusRequest_TM request, ServerCallContext context)
        {
            transactionManager.Logger("I'm Alive");
            var reply = new StatusReply_TM();
            return Task.FromResult(reply);
        }
    }

}
