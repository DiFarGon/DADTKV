using Grpc.Core;
using Grpc.Net.Client;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Client
{
    public class Client
    {
        private string id;
        private string assignedTmId;
        private bool debug;

        private Dictionary<string, string> ids_tmServices = new Dictionary<string, string>(); // <tm_cluster_id, url>
        private GrpcChannel? channel = null;
        private TransactionManagerService.TransactionManagerServiceClient? tmClient;

        /// <summary>
        /// Creates a new Client with given parameters
        /// </summary>
        /// <param name="id"></param>
        /// <param name="assignedTmId"></param>
        /// <param name="debug"></param>
        /// <param name="tms"></param>
        public Client(string id, string assignedTmId, bool debug)
        {
            this.id = id;
            this.assignedTmId = assignedTmId;
            this.debug = debug;

            this.Logger("created");
        }

        /// <summary>
        /// Logs message prefixing it with an identifier
        /// </summary>
        /// <param name="message"></param>
        private void Logger(string message)
        {
            if (debug)
            {
                Console.WriteLine($"[Client {this.id}]\t" + message + '\n');
            }
        }

        public void setTmClusterNodes(string tms)
        {
            this.Logger("set transaction managers");
            string[] keyValuePairs = tms.Split('!', StringSplitOptions.RemoveEmptyEntries);

            foreach (string pair in keyValuePairs)
            {
                string[] parts = pair.Split('-');
                int n = int.Parse(parts[0]);
                string id = parts[1];
                string url = parts[2];

                this.ids_tmServices[id] = url;

                if (id == this.assignedTmId)
                {
                    this.channel = GrpcChannel.ForAddress(url);
                    this.tmClient = new TransactionManagerService.TransactionManagerServiceClient(channel);
                }
            }
        }

        /// <summary>
        /// introduces a wait for a given amount of milisenconds 
        /// </summary>
        /// <param name="line"></param>
        public void handleW(string line)
        {
            // Match one or more digits
            string pattern = @"\d+";

            Match match = Regex.Match(line, pattern);

            this.Logger("Wait " + match.Value);
            Thread.Sleep(int.Parse(match.Value));
        }

        /// <summary>
        /// creates transaction request with set of keys to be read and written
        /// </summary>
        /// <param name="line"></param>
        public void handleT(string line)
        {
            this.Logger("Sending Transaction Request to " + assignedTmId);
            TransactionRequest transactionRequest = new TransactionRequest { ClientId = this.id };

            string pattern = @"\(([^)]*)\)(?:\s*,\s*\(([^)]*)\))?";

            // Match all content inside parentheses.
            MatchCollection matchesInput = Regex.Matches(line, pattern);

            // Extract the matched content into two strings.
            string stringRead = matchesInput.Count > 0 ? matchesInput[0].Groups[1].Value : "";
            string stringWrite = matchesInput.Count > 1 ? matchesInput[1].Groups[1].Value : "";

            Console.WriteLine("Read: " + stringRead);
            Console.WriteLine("Write: " + stringWrite);

            TransactionMessage transactionMessage = new TransactionMessage { };

            //Read keys for transaction
            if (!stringRead.Equals(""))
            {
                string[] readKeys = stringRead.Split(',');
                for (int i = 0; i < readKeys.Length; i++)
                {
                    transactionMessage.KeysRead.Add(readKeys[i].Replace("\"", ""));
                }
            }

            //Write part of transaction
            if (!stringWrite.Equals(""))
            {
                string[] Writeparts = stringWrite.Split(new string[] { ">,<" }, StringSplitOptions.None);
                for (int i = 0; i < Writeparts.Length; i++)
                {
                    string currentString = Writeparts[i].TrimStart('<').TrimEnd('>').Trim();
                    string[] aux = currentString.Split(",");
                    DadIntMessage dadInt = new DadIntMessage { Key = aux[0].Replace("\"", ""), Value = int.Parse(aux[1]) };
                    transactionMessage.DadIntsWrite.Add(dadInt);
                }
            }

            transactionRequest.TransactionMessage = transactionMessage;

            //if after 15 seconds didn't receive reply from server, it tries with another one
            CallOptions callOptions = new CallOptions(deadline: DateTime.UtcNow.AddSeconds(15));
            try
            {
                TransactionResponse response = tmClient.Transaction(transactionRequest, callOptions);
                this.Logger("Received response");
                foreach (DadIntMessage di in response.Read)
                {
                    if (di.Value == int.MinValue)
                    {
                        Console.WriteLine("DadInt" + di.Key + "with value:" + "null");
                    } else
                    {
                        Console.WriteLine("DadInt" + di.Key + "with value:" + di.Value);
                    }
                }
            }
            catch (Exception e)
            {
                this.Logger("Caught exception " + e.Message);
                this.handleException();
                this.handleT(line);
            }
        }

        /// <summary>
        /// sends Status request to his transaction manager
        /// </summary>
        public void handleS()
        {
            this.Logger("Sending Status Request");
            try
            {
                tmClient.Status(new StatusRequest { });
            }
            catch (Exception e)
            {
                this.Logger("Caught exception " + e.Message);
                this.handleException();
                this.handleS();
            }
        }

        public void handleException()
        {
            this.Logger("Handle Exception!");
            Random random = new Random();
            this.ids_tmServices.Remove(assignedTmId);

            // Get a random index based on the remaining keys
            int randomIndex = random.Next(this.ids_tmServices.Count);

            // Access the key at the random index
            this.assignedTmId = this.ids_tmServices.Keys.ElementAt(randomIndex);
            this.channel = GrpcChannel.ForAddress(this.ids_tmServices[this.assignedTmId]);
            this.tmClient = new TransactionManagerService.TransactionManagerServiceClient(channel);
        }

        public async void closeChannel()
        {
            await this.channel.ShutdownAsync();
        }
    }
}