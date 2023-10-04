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

        public override Task<PrepareResponse> Prepare(PrepareRequest request, ServerCallContext context)
        {
            PrepareResponse response = new PrepareResponse();

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
                // the incoming prepare msg has a roundId greater than this lm's RTS, send back promise
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

        public override Task<AcceptResponse> Accept(AcceptRequest request, ServerCallContext context)
        {
            AcceptResponse response = new AcceptResponse();

            int acceptRound = request.RoundId;
            int senderId = request.LmId;

            lock (leaseManager)
            {
                (int, int) readTS = leaseManager.getReadTS();

                leaseManager.Logger($"Accept request received from {senderId}");

                // accept msg received refers to a round in the past of this lm, send nack
                // FIXME: question about this condition in questions folder: "Q1.png"
                if (acceptRound < readTS.Item1 || (acceptRound == readTS.Item1 && senderId < readTS.Item2))
                {
                    leaseManager.Logger($"Accept request refused from {senderId}, acceptRound ({acceptRound}) < readTS {readTS}");
                    response.Ack = false;
                }
                else
                {
                    leaseManager.Logger($"Accept request accepted from {senderId}, acceptRound ({acceptRound}) > readTS {readTS}");
                    leaseManager.setLastAcceptedRound(acceptRound, senderId);

                    // FIXME: this value will change
                    leaseManager.setLastAcceptedValue(request.Value.ToList());

                    leaseManager.broadcastAcceptedMsg();

                    response.Ack = true;
                }
            }
            return Task.FromResult(response);
        }

        public override Task<AcceptedResponse> Accepted(AcceptedRequest request, ServerCallContext context)
        {
            AcceptedResponse response = new AcceptedResponse();

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
                    if (leaseManager.incAcceptedCountCheckIfQuorom())
                    {
                        leaseManager.Logger($"Accepted request accepted from {request.LmId}, acceptedRoundId ({acceptedRound}), received a quorum of accepted so, consensus reached!");
                        response.Ack = true;
                        // FIXME: what does a LM do when it knows a consensus has been reached? Eventually sends the decision to the TMs right?
                        // FIXME: continue here, send decision to TMS!
                    }
                    else
                    {
                        leaseManager.Logger($"Accepted request accepted from {request.LmId}, acceptedRoundId ({acceptedRound}), has received {leaseManager.getAcceptedReceivedCount()} accepteds so far");
                        response.Ack = true;
                    }
                }
            }
            return Task.FromResult(response);
        }
    }
}