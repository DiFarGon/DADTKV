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
        private string tmId;
        private string tms;
        private bool debug;
        private GrpcChannel channel;
        private ClientService.ClientServiceClient client;
        private Dictionary<string, string> ids_tmServices =
            new Dictionary<string, string>();

        /// <summary>
        /// Creates a new Client with given parameters
        /// </summary>
        /// <param name="id"></param>
        /// <param name="tmId"></param>
        /// <param name="debug"></param>
        /// <param name="tms"></param>
        public Client(string id, string tmId, bool debug, string tms)
        {
            this.id = id;
            this.tmId = tmId;
            this.debug = debug;
            this.tms = tms;

            this.Logger("created");
            this.setTmClusterNodes(this.tms);
            Thread.Sleep(1000);
            this.channel = GrpcChannel.ForAddress(this.ids_tmServices[this.tmId]);
            this.client = new ClientService.ClientServiceClient(channel);
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
            string[] keyValuePairs = tms.Split(';', StringSplitOptions.RemoveEmptyEntries);

            foreach (string pair in keyValuePairs)
            {
                string[] parts = pair.Split('-');
                int n = int.Parse(parts[0]);
                string id = parts[1];
                string url = parts[2];

                this.ids_tmServices[id] = url;
            }
            this.Logger("set transaction managers");
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
            this.Logger("Sending Transaction Request to " + tmId);
            TransactionRequest transactionRequest = new TransactionRequest { ClientId = this.id };

            string pattern = @"\(([^)]*)\)(?:\s*,\s*\(([^)]*)\))?";

            // Match all content inside parentheses.
            MatchCollection matchesInput = Regex.Matches(line, pattern);

            // Extract the matched content into two strings.
            string stringRead = matchesInput.Count > 0 ? matchesInput[0].Groups[1].Value : "";
            string stringWrite = matchesInput.Count > 1 ? matchesInput[1].Groups[1].Value : "";

            //Read keys for transaction
            if (!stringRead.Equals(""))
            {
                string[] readKeys = stringRead.Split(',');
                for (int i = 0; i < readKeys.Length; i++)
                {
                    transactionRequest.KeysRead.Add(readKeys[i].Replace("\"", ""));
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
                    DadInt dadInt = new DadInt { Key = aux[0].Replace("\"", ""), Value = int.Parse(aux[1]) };
                    transactionRequest.KeysWrite.Add(dadInt);
                }
            }

            //if after 15 seconds didn't receive reply from server, it tries with another one
            var callOptions = new CallOptions(deadline: DateTime.UtcNow.AddSeconds(15));
            //wait for the response
            try 
            {
                var response = client.Transaction(transactionRequest, callOptions);
                this.Logger("Received response");
                foreach (DadInt dadInt_aux in response.Read)
                {
                    Console.WriteLine("DadInt" + dadInt_aux.Key + "with value:" + dadInt_aux.Value);
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
                client.Status(new StatusRequest { });
            }
            catch (Exception e)
            {
                this.Logger("Caught exception "+ e.Message);
                this.handleException();
                this.handleS();
            }
        }

        public void handleException() 
        {
            this.Logger("Handle Exception!");
            Random random = new Random();
            this.ids_tmServices.Remove(this.tmId);

            // Get a random index based on the remaining keys
            int randomIndex = random.Next(this.ids_tmServices.Count);

            // Access the key at the random index
            this.tmId = this.ids_tmServices.Keys.ElementAt(randomIndex);
            this.channel = GrpcChannel.ForAddress(this.ids_tmServices[this.tmId]);
            this.client = new ClientService.ClientServiceClient(channel);
        }

        public async void closeChannel()
        {
            await this.channel.ShutdownAsync();
        }
    }
}