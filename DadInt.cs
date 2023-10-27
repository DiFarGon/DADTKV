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

        /// <summary>
        /// Creates a DadInt with the given key and value
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public DadInt(string key, int value)
        {
            this.Key = key;
            this.Value = value;
        }

        /// <summary>
        /// Returns a string representation of this DadInt instance
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return $"({this.Key}: {this.Value})";
        }
    }

}
