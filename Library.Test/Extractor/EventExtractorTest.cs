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
            USqlColumn<string> col1 = new USqlColumn<string>("app_id");
            USqlColumn<string> col2 = new USqlColumn<string>("platform");
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
                Assert.IsTrue(result[0].Schema.Count == 2);
                //Verify the result
                Assert.IsTrue(result[0].Get<string>("app_id") == "angry-bird");
                Assert.IsTrue(result[0].Get<string>("platform") == "web");
            }
        }

        
        
    }
}
