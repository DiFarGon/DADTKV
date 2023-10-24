using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DadInt
{
    public class DadInt
    {
        public readonly string Key;
        public int Value;

        public DadInt(string key, int value)
        {
            this.Key = key;
            this.Value = value;
        }
    }
}
