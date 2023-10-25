using System;
using Google.Protobuf.Collections;
using System.Collections.Generic;
using System.Linq;
using Grpc.Core;
using System.Transactions;

namespace LeaseManager
{
    public class LeaseManagerServiceImpl : LeaseManagerService.LeaseManagerServiceBase
    {
        LeaseManager leaseManager;

        public LeaseManagerServiceImpl(LeaseManager leaseManager)
        {
            this.leaseManager = leaseManager;
        }

        public override Task<LeaseResponse> Lease(LeaseRequest request, ServerCallContext context)
        {
            leaseManager.registerLease(request.TmId, request.Keys.ToList());
            LeaseResponse response = new LeaseResponse();
            return Task.FromResult(response);
        }

        public override Task<PrepareResponse> Prepare(PrepareRequest request, ServerCallContext context)
        {
            PrepareResponse response = new PrepareResponse();

            PaxosNode paxosNode = leaseManager.getPaxosNode();

            if (request.BallotId > paxosNode.getMostRecentReadTS())
            {
                response.Ok = true;
                foreach (int instanceId in request.UnresolvedInstances)
                {
                    paxosNode.getInstanceState(instanceId).setRTS(request.BallotId);
                    response.InstancesStates.Add(instanceId, PaxosNode.instanceStateToInstanceStateMessage(paxosNode.getInstanceState(instanceId)));
                }

                paxosNode.setMostRecentReadTS(request.BallotId);
                paxosNode.setRoundId((request.BallotId - request.Id) / paxosNode.getClusterSize());

                if (paxosNode.isLeader())
                    paxosNode.setLeader(false);
            }
            else
            {
                response.Ok = false;
                response.MostRecentReadTS = paxosNode.getMostRecentReadTS();
            }
            return Task.FromResult(response);
        }

        public override Task<AcceptResponse> Accept(AcceptRequest request, ServerCallContext context)
        {
            AcceptResponse response = new AcceptResponse();

            PaxosNode paxosNode = leaseManager.getPaxosNode();
            InstanceState instanceState = paxosNode.getInstanceState(request.InstanceId);

            if (request.BallotId == paxosNode.getMostRecentReadTS())
                paxosNode.setLastKnownLeader(request.Id);

            if (request.BallotId == instanceState.getRTS())
            {
                response.Ok = true;
                instanceState.setWTS(request.BallotId);

                if (request.Value == null)
                    instanceState.setNo_op(true);
                else
                    instanceState.setValue(PaxosNode.LeasesListMessageToLeasesList(request.Value));
            }
            else
            {
                response.Ok = false;
                response.MostRecentReadTS = instanceState.getRTS();
            }
            return Task.FromResult(response);
        }

        public override Task<DecidedResponse> Decided(DecidedRequest request, ServerCallContext context)
        {
            DecidedResponse response = new DecidedResponse();

            PaxosNode paxosNode = leaseManager.getPaxosNode();
            InstanceState instanceState = paxosNode.getInstanceState(request.InstanceId);
            instanceState.setDecided(true);

            return Task.FromResult(response);
        }

    }
}