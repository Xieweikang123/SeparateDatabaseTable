using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Models
{
    public class BaseModel
    {
        public Guid Id { get; set; }
        public DateTime AddTime {
            get;
            set;
        }

    }
}
