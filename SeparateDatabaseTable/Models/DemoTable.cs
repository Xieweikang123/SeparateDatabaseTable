using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SeparateDatabaseTable.Models
{
    public sealed class DemoTable:BaseModel
    {
        public Guid Id { get; set; }
        public string Value { get; set; }
    }
}
