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
    }
}