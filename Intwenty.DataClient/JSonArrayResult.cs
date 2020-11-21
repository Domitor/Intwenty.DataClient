using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Intwenty.DataClient
{

    public class JsonArrayResult : IJsonArrayResult
    {

        public int ObjectCount { get; set; }

        public int FirstObjectId { get; set; }

        public int LastObjectId { get; set; }

        public double Duration { get; set; }

        public List<IJsonObjectResult> Objects { get; set; } 

        public string Data
        {
            get
            {
                if (Objects == null)
                    return "[]";
                if (Objects.Count == 0)
                    return "[]";

                return GetData().ToString();
            }
        }

        private StringBuilder GetData()
        {
            var res = new StringBuilder();
            res.Append("[");
            for(var i=0; i < Objects.Count; i++)
            {
                if (i == 0)
                {
                    res.Append(Objects[i].Data);
                }
                else
                {
                    res.Append(',' + Objects[i].Data);
                }
            }
            res.Append("]");

            return res;
        }

        public JsonArrayResult()
        {
            Objects = new List<IJsonObjectResult>(); 
        }

     


    }
}
