using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Intwenty.DataClient
{
    public class JsonObjectResult : IJsonObjectResult
    {
        public List<IResultSetValue> Values { get; set; }

        public string GetJsonString() 
        {
            if (string.IsNullOrEmpty(pStringData))
                return "{}";


            return pStringData;
            
        }

        private string pStringData { get; set; }

        public void SetData(string data)
        {
            pStringData = data;
        }

        public JsonObjectResult()
        {
            Values = new List<IResultSetValue>();
        }

    }

}
