using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Intwenty.DataClient
{

    public class JsonArrayResult : IJsonArrayResult
    {

        public int ObjectCount { get; set; }

        public double Duration { get; set; }

        public List<IJsonObjectResult> JsonObjects { get; set; } 

        public string GetJsonString()
        {
            
            if (JsonObjects == null)
                return "[]";
            if (JsonObjects.Count == 0)
                return "[]";

            var res = new StringBuilder();
            res.Append("[");
            for (var i = 0; i < JsonObjects.Count; i++)
            {
                if (i == 0)
                {
                    res.Append(JsonObjects[i].GetJsonString());
                }
                else
                {
                    res.Append(',' + JsonObjects[i].GetJsonString());
                }
            }
            res.Append("]");

            return res.ToString();
        }

        public JsonArrayResult()
        {
            JsonObjects = new List<IJsonObjectResult>(); 
        }

    }
}
