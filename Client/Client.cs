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
        private string tmUrl;
        private bool debug;
        private readonly GrpcChannel channel;
        private readonly ClientService.ClientServiceClient client;

        public Client(string id, string tmUrl, bool debug)
        {
            this.id = id;
            this.tmUrl = tmUrl;
            this.debug = debug;

            this.Logger("created");
            Thread.Sleep(1000);
            this.channel = GrpcChannel.ForAddress(this.tmUrl);
            this.client = new ClientService.ClientServiceClient(channel);
        }

        private void Logger(string message)
        {
            if (debug)
            {
                Console.WriteLine($"[Client {this.id}]\t" + message + '\n');
            }
        }

        public void handleW(string line)
        {
            Thread.Sleep(line[1]);
        }

        public void handleT(string line)
        {
            this.Logger("Sending Transaction Request");
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
            //doubt, if i have to catch the reply or not, since it is an empty reply
            client.Transaction(transactionRequest);
        }

        public void handleS()
        {
            this.Logger("Sending Status Request");
            client.Status(new StatusRequest { });
        }

        public async void closeChannel()
        {
            await this.channel.ShutdownAsync();
        }
    }
}