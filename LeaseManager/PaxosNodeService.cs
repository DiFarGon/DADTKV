using Grpc.Core;

namespace LeaseManager
{
    public class PaxosNodeServiceImpl : PaxosNodeService.PaxosNodeServiceBase
    {
        PaxosNode paxosNode;

        public PaxosNodeServiceImpl(PaxosNode paxosNode)
        {
            this.paxosNode = paxosNode;
        }

        public override Task<PrepareResponse> Prepare(PrepareRequest request, ServerCallContext context)
        {
            int prepareRound = request.RoundId;
            int prepareNodeId = request.Id;

            (int, int) lastPromisedRound = paxosNode.getLastPromisedRoundId();
            (int, int) lastAcceptedRound = paxosNode.getLastAcceptedRoundId();
            List<Lease> lastAcceptedValue = paxosNode.getLastAcceptedValue();

            if (prepareRound > lastPromisedRound.Item1 || (prepareRound == lastPromisedRound.Item1 && prepareNodeId > lastPromisedRound.Item2))
            {
                paxosNode.setLastPromisedRound((prepareRound, prepareNodeId));
                return Task.FromResult(new PrepareResponse { Ok = true, AcceptedRoundId = lastAcceptedRound.Item1, AcceptedNodeId = lastAcceptedRound.Item2, AcceptedValue = { lastAcceptedValue } }); //FIXME:
            }
            else
            {
                return Task.FromResult(new PrepareResponse { Ok = false, LastPromisedRoundId = lastPromisedRound.Item1 });
            }
        }

        public override Task<AcceptResponse> Accept(AcceptRequest request, ServerCallContext context)
        {
            int acceptRound = request.RoundId;
            int acceptNodeId = request.Id;

            (int, int) lastPromisedRound = paxosNode.getLastPromisedRoundId();

            if (acceptRound >= lastPromisedRound.Item1 || (acceptRound == lastPromisedRound.Item1 && acceptNodeId >= lastPromisedRound.Item2))
            {
                paxosNode.setLastAcceptedRound((acceptRound, acceptNodeId));
                paxosNode.setLastAcceptedValue(request.Value); // FIXME:

                paxosNode.broadcastAccepted();
                return Task.FromResult(new AcceptResponse { Ok = true });
            }
            else
            {
                return Task.FromResult(new AcceptResponse { Ok = false, LastPromisedRoundId = lastPromisedRound.Item1 });
            }
        }

        public override Task<AcceptedResponse> Accepted(AcceptedRequest request, ServerCallContext context)
        {
            int acceptedRound = request.RoundId;
            int acceptedNodeId = request.Id;

            (int, int) lastPromisedRound = paxosNode.getLastPromisedRoundId();

            if (acceptedRound >= lastPromisedRound.Item1 || (acceptedRound == lastPromisedRound.Item1 && acceptedNodeId >= lastPromisedRound.Item2))
            {
                paxosNode.setLastAcceptedRound((acceptedRound, acceptedNodeId));
                paxosNode.setLastAcceptedValue(request.Value); // FIXME:

                paxosNode.incrAcceptedCount();
                if (paxosNode.getAcceptedCount() > paxosNode.getClusterSize() / 2)
                    paxosNode.instanceDecided();

                return Task.FromResult(new AcceptedResponse { Ok = true });
            }
            else
            {
                return Task.FromResult(new AcceptedResponse { Ok = false });
            }
        }
    }
}