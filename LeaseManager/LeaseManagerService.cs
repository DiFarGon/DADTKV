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

        public override Task<ControlLMResponse> ControlLM(ControlLMRequest request, ServerCallContext context)
        {
            ControlLMResponse response = new ControlLMResponse();
            response.LmId = leaseManager.getClusterId();

            Console.WriteLine($"[LeaseManager {leaseManager.getClusterId()}]\tControl request received from {request.LmId}");

            return Task.FromResult(response);
        }

        public override Task<LeaseResponse> Lease(LeaseRequest request, ServerCallContext context)
        {
            LeaseResponse response = new LeaseResponse();

            leaseManager.Logger($"Received lease request from {request.TmId}");

            return Task.FromResult(response);
        }

        public override Task<Prepare1Response> Prepare1(Prepare1Request request, ServerCallContext context)
        {
            Prepare1Response response = new Prepare1Response();

            int prepareRound = request.RoundId;
            int senderId = request.LmId;

            lock (leaseManager)
            {
                (int, int) readTS = leaseManager.getReadTS();
                (int, int) writeTS = leaseManager.getWriteTS();

                leaseManager.Logger($"Prepare request received from (P{senderId})");

                // the roundId of the prepare is not the greatest roundId this lm has seen (is not the greatest RTS), send back nack(RTS)
                if ((prepareRound < readTS.Item1) || (prepareRound == readTS.Item1 && senderId < readTS.Item2))
                {
                    leaseManager.Logger($"Prepare request refused from (P{senderId}), roundId ({prepareRound}, {senderId}) < readTS {readTS}");
                    response.Ack = false;
                    response.LastPromisedRound = readTS.Item1;
                    response.LastAcceptedRoundNodeId = readTS.Item2;

                }
                // the incoming prepare msg has a roundId greater than this node's RTS, send back promise
                else
                {
                    leaseManager.Logger($"Prepare request accepted from {senderId}, roundId ({prepareRound}, {senderId}) > readTS {readTS}");
                    leaseManager.setLastPromisedRound(prepareRound, senderId);

                    response.Ack = true;
                    if (writeTS.Item1 != 0)
                    {
                        leaseManager.Logger($"In sent promise, lastAcceptedRoundId is {writeTS}");
                        response.LastAcceptedRound = writeTS.Item1;
                        response.LastAcceptedRoundNodeId = writeTS.Item2;
                        response.LastAcceptedValue.AddRange(leaseManager.getLastAcceptedValue()); // TODO: understand what value should be exactly (list of leases? leases in queue and leases granted(state)?)
                    }
                }
            }
            return Task.FromResult(response);
        }

        public override Task<Accept1Response> Accept1(Accept1Request request, ServerCallContext context)
        {
            Accept1Response response = new Accept1Response();

            int acceptRound = request.RoundId;
            int senderId = request.LmId;

            lock (leaseManager)
            {
                (int, int) readTS = leaseManager.getReadTS();

                leaseManager.Logger($"Accept request received from {senderId}");

                if (acceptRound < readTS.Item1 || (acceptRound == readTS.Item1 && senderId < readTS.Item2))
                {
                    leaseManager.Logger($"Accept request refused from {senderId}, acceptRound ({acceptRound}) < readTS {readTS}");
                    response.Ack = false;
                    response.LastPromisedRoundId = readTS.Item1;
                }
                else
                {
                    leaseManager.Logger($"Accept request accepted from {senderId}, acceptRound ({acceptRound}) > readTS {readTS}");
                    leaseManager.setLastAcceptedRound(acceptRound, senderId);

                    // FIXME: the value is still TBD, what should it be?
                    leaseManager.setLastAcceptedValue(request.Value.ToList());

                    leaseManager.broadcastAcceptedMsg();

                    response.Ack = true;
                }
            }
            return Task.FromResult(response);
        }

        public override Task<Accepted1Response> Accepted1(Accepted1Request request, ServerCallContext context)
        {
            Accepted1Response response = new Accepted1Response();

            int acceptedRound = request.RoundId;
            int senderId = request.LmId;

            lock (leaseManager)
            {
                (int, int) readTS = leaseManager.getReadTS();

                leaseManager.Logger($"Accepted received from P{senderId}");

                //  TODO: not sure if this case is required
                if (acceptedRound < readTS.Item1 || (acceptedRound == readTS.Item1 && senderId < readTS.Item2))
                {
                    leaseManager.Logger($"Accepted request refused from {senderId}, acceptedRoundId ({acceptedRound}) does not refer to readTS {readTS}");
                    response.Ack = false;
                }
                else
                {
                    response.Ack = true;
                    leaseManager.incAcceptedCount();
                    if (leaseManager.isAcceptedQuorom())
                    {
                        leaseManager.Logger($"Accepted request accepted from {request.LmId}, acceptedRoundId ({acceptedRound}), received a quorum of accepted so, consensus reached!");
                        leaseManager.setDecided(true);
                        // FIXME: what does a LM do when it knows a consensus has been reached? Eventually sends the decision to the TMs right?
                        // FIXME: continue here, send decision to TMS!
                    }
                    else
                    {
                        leaseManager.Logger($"Accepted request accepted from {request.LmId}, acceptedRoundId ({acceptedRound}), has received {leaseManager.getAcceptedReceivedCount()} accepteds so far");

                    }
                }
            }
            return Task.FromResult(response);
        }
    }
}