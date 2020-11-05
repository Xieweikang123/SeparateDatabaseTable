using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SeparateDatabaseTable.Models
{
    public class BaseModel
    {
        public DateTime CreateTime {
            get { return DateTime.Now; }
        }
    }
}
