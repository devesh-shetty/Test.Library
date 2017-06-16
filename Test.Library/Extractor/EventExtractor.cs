using Microsoft.Analytics.Interfaces;
using Microsoft.Analytics.Types.Sql;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Test.Library.Extractor
{
    public class EventExtractor : IExtractor
    {
        public override IEnumerable<IRow> Extract(IUnstructuredReader input, IUpdatableRow output)
        {
            //process TSV
            char columnDelimiter = '\t';
            string line;
            var reader = new StreamReader(input.BaseStream);
            while ((line = reader.ReadLine()) != null)
            {
                var tokens = line.Split(columnDelimiter);
                output.Set("app_id", tokens[0]);
                output.Set("platform", tokens[1]);
                yield return output.AsReadOnly();
            }
        }
    }
}