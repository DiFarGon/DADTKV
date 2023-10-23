using System;
using Google.Protobuf.Collections;
using System.Collections.Generic;
using System.Linq;
using Grpc.Core;

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
            PaxosNode paxosNode = leaseManager.getPaxosNode();

            PrepareResponse response = new PrepareResponse();

            if (request.BallotId > paxosNode.getMostRecentReadTS())
            {
                response.Ok = true;
                foreach (int instanceId in request.UnresolvedInstances)
                {
                    response.InstancesStates.Add(instanceId, PaxosNode.instanceStateToGrpcInstanceState(paxosNode.getInstanceState(instanceId)));
                }
                paxosNode.setMostRecentReadTS(request.BallotId);
                paxosNode.setRoundId((request.BallotId - request.Id) / paxosNode.getClusterSize());
                paxosNode.setLastKnownLeader(request.Id);
                if (paxosNode.isLeader())
                {
                    paxosNode.setLeader(false);
                }
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
            PaxosNode paxosNode = leaseManager.getPaxosNode();

            AcceptResponse response = new AcceptResponse();

            if (request.BallotId == paxosNode.getMostRecentReadTS())
            {
                response.Ok = true;
                paxosNode.setInstanceStateWriteTS(request.InstanceId, request.BallotId);
                if (request.Value == null)
                    paxosNode.setInstanceStateValue(request.InstanceId, null);
                else
                    paxosNode.setInstanceStateValue(request.InstanceId, PaxosNode.grpcLeasesListToLeasesList(request.Value));
            }
            else
            {
                response.Ok = false;
                response.MostRecentReadTS = paxosNode.getMostRecentReadTS();
            }

            return Task.FromResult(response);
        }

        public override Task<DecidedResponse> Decided(DecidedRequest request, ServerCallContext context)
        {
            PaxosNode paxosNode = leaseManager.getPaxosNode();

            DecidedResponse response = new DecidedResponse();

            paxosNode.setInstanceStateDecided(request.InstanceId);

            return Task.FromResult(response);
        }
    }
}