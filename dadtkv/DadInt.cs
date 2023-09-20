using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace dadtkv
{
    internal class DadInt
    {
        private string key;
        private int value;

        public DadInt(string key, int value)
        {
            this.key = key;
            this.value = value;
        }

        public string getKey() { return this.key; }

        public int getValue() { return this.value; }

        public void setValue(int value) { this.value = value; }
    }
}
