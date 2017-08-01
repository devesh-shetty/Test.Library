using Microsoft.Analytics.Interfaces;
using Microsoft.Analytics.Types.Sql;
using Microsoft.Analytics.UnitTest;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Test.Library.Extractor;
using System.Diagnostics;
using Newtonsoft.Json;

namespace Library.Test.Extractor
{
    [TestClass]
    public class EventExtractorTest
    {
        private TestContext testContextInstance;

        /// <summary>
        ///Gets or sets the test context which provides
        ///information about and functionality for the current test run.
        ///</summary>
        public TestContext TestContext
        {
            get
            {
                return testContextInstance;
            }
            set
            {
                testContextInstance = value;
            }

        }
        
        public IRow RowGenerator()
        {
            //generate the schema
            USqlColumn<string> col1 = new USqlColumn<string>("contexts");
            USqlColumn<string> col2 = new USqlColumn<string>("contexts.data.author");
            List<IColumn> columns = new List<IColumn> { col1, col2 };
            USqlSchema schema = new USqlSchema(columns);
            return new USqlRow(schema, null);
        }

        [TestMethod]
        [DeploymentItem(@"Input\test.txt")]
        public void TestMyExtractor()
        {
            IUpdatableRow output = RowGenerator().AsUpdatable();
            var testDataPath = "\"" + Directory.GetCurrentDirectory() + @"\test.txt" + "\"";
            using (FileStream stream = new FileStream(@"test.txt", FileMode.Open))
            {
                //Read input file 
                USqlStreamReader reader = new USqlStreamReader(stream);
                //Run the UDO
                EventExtractor extractor = new EventExtractor();
                List<IRow> result = extractor.Extract(reader, output).ToList();
                //Verify the schema
                //Assert.IsTrue(result[0].Schema.Count == 2);
                //Verify the result
                //Console.WriteLine("Result: "+result[0].Get<string>("contexts"));
                //Assert.IsTrue(result[0].Get<string>("contexts") == "yolo");
                //Assert.IsTrue(result[0].Get<string>("platform") == "web");
                //var contexts = result[0].Get<string>("contexts");
                //var schema = JsonFunctions.JsonTuple(contexts, "data[*]");
                //schema.Values.ToList().ForEach(item => {
                //  var innerData = JsonFunctions.JsonTuple(item, "data");
                //Console.WriteLine("innerdata length: "+ innerData.Count());
                //innerData.Values.ToList().ForEach(innerItem => { 
                //  var genre =   JsonFunctions.JsonTuple(innerItem, "navigationStart");
                // genre.Values.ToList().ForEach( g => Console.WriteLine(g));
                //}
                //  );


                //});

                var sqlMap = result[0].Get<SqlMap<string, string>>("contexts.data.author");

                var keys = sqlMap.Keys;
                keys.ToList().ForEach(key => Console.WriteLine($"{key}: {sqlMap[key]}"));

                //var data = schema.Values.ElementAt(0);
                //var innerData = JsonFunctions.JsonTuple(data, "data");
            }
        }

        
        
    }
}
