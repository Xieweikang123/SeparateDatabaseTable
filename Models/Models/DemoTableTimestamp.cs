using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Models
{
    public sealed class DemoTableTimestamp : BaseModel
    {
       
        public string Value { get; set; }
        public ulong Timestamp { get; set; }
    }
}
