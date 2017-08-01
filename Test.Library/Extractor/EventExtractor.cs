using Microsoft.Analytics.Interfaces;
using Microsoft.Analytics.Types.Sql;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
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
            string line;
            char columnDelimiter = '\t';

            var reader = new StreamReader(input.BaseStream);
            while ((line = reader.ReadLine()) != null)
            {
                var tokens = line.Split(columnDelimiter);
                ExtractJson(tokens[0], output);
                //output.Set("contexts", tokens[0]);
                yield return output.AsReadOnly();
            }
        }

        private void ExtractJson(string json, IUpdatableRow output)
        {
            //var jsonReader = new JsonTextReader(new StringReader(tokens[0]));
            var jsonReader = new JsonTextReader(new StringReader(json));
            while (jsonReader.Read())
            {
                if (jsonReader.TokenType == JsonToken.StartObject)
                {
                    var token = JToken.Load(jsonReader);
                    // Rows
                    //  All objects are represented as rows
                    foreach (JObject o in token.SelectTokens("data[*]"))
                    {
                        //Console.WriteLine($"JObject: {o}");
                        // All fields are represented as columns
                        //this.JObjectToRow(o, output);
                         //mapToColumns(o, output);
                         //yield return output.AsReadOnly();
                        //var tokens = o.SelectTokens("$.data.author");
                        var sqlMap = SqlMap.Create(MapHelper<string>(o, "$.data.author"));
                        if (sqlMap != null && sqlMap.Count > 0) {
                            Console.WriteLine("SqlMap exists");
                            var tempMap = output.Get<SqlMap<string, string>>("contexts.data.author");

                            if (tempMap != null && tempMap.Count > 0)
                            {
                                Console.WriteLine("tempMAp exists");

                                var combinedDict = new Dictionary<string, string>();
                                tempMap.Keys.ToList().ForEach(key => combinedDict.Add(key, tempMap[key]));
                                sqlMap.Keys.ToList().ForEach(key => combinedDict.Add(key, sqlMap[key]));

                                output.Set("contexts.data.author", new SqlMap<string, string>(combinedDict));
                            }
                            else {
                                output.Set("contexts.data.author", sqlMap);
                            }

                        }

                    }
                }
            }
        }

        private static IEnumerable<KeyValuePair<string, T>> MapHelper<T>(JToken root, string path)
        {
            // Children
            var children = SelectChildren<T>(root, path);
            //Console.WriteLine($"Childrem count: {children.Count()}");
            //children.ToList().ForEach(item => Console.WriteLine(item));
            foreach (var token in children)
            {
                // Token => T
                var value = (T)JsonFunctions.ConvertToken(token, typeof(T));

                //Console.WriteLine($"tokenPAth: {token.Path},  value: {value}");
                // Tuple(path, value)
                yield return new KeyValuePair<string, T>(token.Path, value);
            }
        }

        /// <summary/>
        private static IEnumerable<JToken> SelectChildren<T>(JToken root, string path)
        {
            // Path specified
            if (!string.IsNullOrEmpty(path))
            {
                return root.SelectTokens(path);
            }

            // Single JObject
            var o = root as JObject;
            if (o != null)
            {
                //  Note: We have to special case JObject.
                //      Since JObject.Children() => JProperty.ToString() => "{"id":1}" instead of value "1".
                return o.PropertyValues();
            }

            // Multiple JObjects
            return root.Children();
        }

        private void mapToColumns(JObject obj, IUpdatableRow output)
        {
            var json = JsonConvert.SerializeObject(obj);
            var genre = JsonFunctions.JsonTuple(json, "$.data.author");
            //Console.WriteLine("Genre: "+genre.Count());
            /*
            genre.Values.ToList().ForEach(g => {
                //Console.WriteLine(g);
                output.Set("contexts.data.genre", g);
                }
                );
            */
            var keys = genre.Keys;
            foreach (var key in keys)
            {
                Console.WriteLine($"{key}: {genre[key]}");
            }

            //Console.WriteLine($"data: {genre["data.breadcrumb"]}");

            //setting it to null for second object
            //work on it
            //create a condition and add second object if not null

           output.Set("contexts.data.genre", genre["data.genre"]);
        }

        /// <summary/>
        protected virtual void JObjectToRow(JObject o, IUpdatableRow row)
        {
            foreach (var c in row.Schema)
            {
                JToken token = null;
                object value = c.DefaultValue;

                // All fields are represented as columns
                //  Note: Each JSON row/payload can contain more or less columns than those specified in the row schema
                //  We simply update the row for any column that matches (and in any order).
                if (o.TryGetValue(c.Name, out token) && token != null)
                {
                    // Note: We simply delegate to Json.Net for all data conversions
                    //  For data conversions beyond what Json.Net supports, do an explicit projection:
                    //      ie: SELECT DateTime.Parse(datetime) AS datetime, ...
                    //  Note: Json.Net incorrectly returns null even for some non-nullable types (sbyte)
                    //      We have to correct this by using the default(T) so it can fit into a row value
                    value = JsonFunctions.ConvertToken(token, c.Type) ?? c.DefaultValue;
                }

                // Update
                row.Set<object>(c.Name, value);
            }
        }
    }

}
