using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
            var reply = new TransactionReply();
            return Task.FromResult(reply);
        }

        public override Task<StatusReply> Status(StatusRequest statusRequest, ServerCallContext context)
        {
            transactionManager.Logger("I'm Alive");
            var reply = new StatusReply();
            return Task.FromResult(reply);
        }
    }
}
