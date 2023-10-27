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
            leaseManager.Logger("Received Lease Request from " + request.TmId + "\n" + request.Keys + "\n");

            leaseManager.registerLease(request.TmId, request.Keys.ToList());
            LeaseResponse response = new LeaseResponse();
            return Task.FromResult(response);
        }

        public override Task<PrepareResponse> Prepare(PrepareRequest request, ServerCallContext context)
        {
            PrepareResponse response = new PrepareResponse();

            PaxosNode paxosNode = leaseManager.getPaxosNode();

            if (paxosNode.getFailureSuspicions().ContainsKey(request.InstanceId) && paxosNode.getFailureSuspicions(request.InstanceId).Contains(request.Id))
            {
                leaseManager.Logger($"Received Accept Request from {request.Id} for instance {request.InstanceId} but simulating that didn't receive\n");
                return Task.FromResult(new PrepareResponse() { NotReceived = true });
            }

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

                paxosNode.setLastKnownLeader(request.Id);
                if (paxosNode.isLeader())
                    paxosNode.setLeader(false);
            }
            else
            {
                response.Ok = false;
                response.MostRecentReadTS = paxosNode.getMostRecentReadTS();
            }

            leaseManager.Logger($"Received Prepare Request from {request.Id} on instance {request.InstanceId}. Ack: {response.Ok} \n");

            return Task.FromResult(response);
        }

        public override Task<AcceptResponse> Accept(AcceptRequest request, ServerCallContext context)
        {
            PaxosNode paxosNode = leaseManager.getPaxosNode();

            if (paxosNode.getFailureSuspicions().ContainsKey(request.InstanceId))
            {
                leaseManager.Logger($"Failure suspicions for instance {request.InstanceId}: {string.Join(",", paxosNode.getFailureSuspicions(request.InstanceId))}\n");
            }

            if (paxosNode.getFailureSuspicions().ContainsKey(request.InstanceId) && paxosNode.getFailureSuspicions(request.InstanceId).Contains(request.Id))
            {
                leaseManager.Logger($"Received Accept Request from {request.Id} for instance {request.InstanceId} but simulating that didn't receive\n");
                return Task.FromResult(new AcceptResponse() { NotReceived = true });
            }

            InstanceState instanceState = paxosNode.getInstanceState(request.InstanceId);

            AcceptResponse response = new AcceptResponse()
            {
                Id = paxosNode.getId(),
                InstanceId = request.InstanceId,
            };

            leaseManager.Logger($"On rcv accept request from {request.Id} for isntance {request.InstanceId}, incoming ballorit: {request.BallotId}, current rts: {instanceState.getRTS()}\n  ");

            if (request.BallotId > paxosNode.getMostRecentReadTS())
            {
                paxosNode.setLastKnownLeader(request.Id);
                if (paxosNode.isLeader())
                    paxosNode.setLeader(false);

            }

            if (request.BallotId >= instanceState.getRTS())
            {
                response.Ok = true;
                instanceState.setWTS(request.BallotId);
                instanceState.setValue(PaxosNode.LeasesListMessageToLeasesList(request.Value));
                instanceState.setNoOp(false);
            }
            else
            {
                response.Ok = false;
                response.MostRecentReadTS = instanceState.getRTS();
            }

            leaseManager.Logger($"Received Accept Request from {request.Id} for instance {request.InstanceId}. Value: {LeaseManager.LeasesListToString(PaxosNode.LeasesListMessageToLeasesList(request.Value))}. ack: {response.Ok} \n");

            return Task.FromResult(response);
        }

        public override Task<DecidedResponse> Decided(DecidedRequest request, ServerCallContext context)
        {
            leaseManager.Logger($"Received Decided Request from {request.Id} for instance {request.InstanceId}. Value: {LeaseManager.LeasesListToString(PaxosNode.LeasesListMessageToLeasesList(request.Value))} \n");

            DecidedResponse response = new DecidedResponse();

            PaxosNode paxosNode = leaseManager.getPaxosNode();

            if (request.BallotId > paxosNode.getMostRecentReadTS())
            {
                paxosNode.setLastKnownLeader(request.Id);
                if (paxosNode.isLeader())
                    paxosNode.setLeader(false);

            }

            if (paxosNode.getFailureSuspicions().ContainsKey(request.InstanceId) && paxosNode.getFailureSuspicions(request.InstanceId).Contains(request.Id))
            {
                leaseManager.Logger($"Received Decided Request from {request.Id} for instance {request.InstanceId} but simulating that didn't receive\n");
                return Task.FromResult(new DecidedResponse() { NotReceived = true });
            }

            InstanceState instanceState = paxosNode.getInstanceState(request.InstanceId);
            instanceState.setDecided(true);

            paxosNode.updateLeasesQueue(PaxosNode.LeasesListMessageToLeasesList(request.Value));

            Console.WriteLine($"Instance {request.InstanceId}:\n");
            foreach (KeyValuePair<int, InstanceState> kvp in paxosNode.getInstancesStates())
            {
                Console.WriteLine($"instance {kvp.Key}: < {kvp.Value.getRTS()}, {kvp.Value.getWTS()}, {LeaseManager.LeasesListToString(kvp.Value.getValue())}> decided: {kvp.Value.isDecided()}\n");
            }

            return Task.FromResult(response);
        }

    }
}