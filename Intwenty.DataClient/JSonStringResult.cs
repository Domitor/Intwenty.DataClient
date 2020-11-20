using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Intwenty.DataClient
{
    public class JSonStringResult : IJSonStringResult
    {
        public bool IsArray { get; set; }

        public int ObjectCount { get; set; }

        public int LastRecordId { get; set; }

        public int FirstRecordId { get; set; }

        public double Duration { get; set; }

        public string Data 
        {
            get 
            {
                if (StringData == null && IsArray)
                    return "[]";
                if (StringData == null && !IsArray)
                    return "{}";

                return StringData.ToString();
            }
        }

        private StringBuilder StringData { get; set; }

        public void SetData(StringBuilder data)
        {
            StringData = data;
        }

        public JSonStringResult()
        {
           
        }

     
    }
}
