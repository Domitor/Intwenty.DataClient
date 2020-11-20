using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Intwenty.DataClient
{
    public class JsonObjectResult : IJsonObjectResult
    {
        public int ObjectId { get; set; }

        public string Data 
        {
            get 
            {
                if (string.IsNullOrEmpty(pStringData))
                    return "{}";


                return pStringData;
            }
        }

        private string pStringData { get; set; }

        public void SetData(string data)
        {
            pStringData = data;
        }

    }

}
