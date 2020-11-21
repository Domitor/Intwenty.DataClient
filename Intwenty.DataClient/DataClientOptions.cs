using System;
using System.Collections.Generic;
using System.Text;

namespace Intwenty.DataClient
{
    public enum JsonNullValueMode { Exclude, Include };

    public class DataClientOptions
    {
        public JsonNullValueMode JsonNullValueHandling { get; set; }
    }
}
