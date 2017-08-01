using System;
using System.Linq;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Microsoft.Analytics.Types.Sql;

namespace Test.Library.Extractor
{
    /// <summary>
    /// JsonFunctions (sample)
    ///
    /// </summary>
    public static class JsonFunctions
    {
        /// <summary>
        /// JsonTuple("json", [$e1], [$e2], ...)
        ///     1. Parse Json (once for all paths)
        ///     2. Apply the path expressions
        ///     3. Tuples are returned as MAP[path, value]
        ///             Path  = Path of resolved node (matching the expression)
        ///             Value = Node contents (of the matching nodes)
        ///   ie:
        ///     JsonTuple(json, "id", "name")              -> field names          MAP{ {id, 1 }, {name, Ed } }
        ///     JsonTuple(json, "$.address.zip")           -> nested fields        MAP{ {address.zip, 98052}  }
        ///     JsonTuple(json, "$..address")              -> recursive children   MAP{ {address, 98052}, {order[0].address, 98065}, ...           }
        ///     JsonTuple(json, "$[?(@.id > 1)].id")       -> path expression      MAP{ {id, 2 }, {order[7].id, 4}, ...                            }
        ///     JsonTuple(json)                            -> children             MAP{ {id, 1 }, {name, Ed}, { email, donotreply@live,com }, ...  }
        /// </summary>
        public static SqlMap<string, string> JsonTuple(string json, params string[] paths)
        {
            // Delegate
            return JsonTuple<string>(json, paths);
        }

        /// <summary/>
        public static SqlMap<string, T> JsonTuple<T>(string json, params string[] paths)
        {
            // Parse (once)
            //  Note: Json.Net NullRefs on <null> input Json
            //        Given <null> is a common column/string value, map to empty set for composability
            var root = string.IsNullOrEmpty(json) ? new JObject() : JToken.Parse(json);
            //Console.WriteLine($"Root: {root}");
            //Console.WriteLine($"Path length: {paths.Length}, path: {paths[0]}");
            // Apply paths
            if (paths != null && paths.Length > 0)
            {
                var schemaMap = SqlMap.Create(paths.SelectMany(path => ApplyPath<T>(root, path)));
              //  Console.WriteLine($"Schema Map Count: {schemaMap.Count}");
                   
                return schemaMap;
            }

            // Children
            return SqlMap.Create(ApplyPath<T>(root, null));
        }

        /// <summary/>
        private static IEnumerable<KeyValuePair<string, T>> ApplyPath<T>(JToken root, string path)
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

        /// <summary/>
        internal static string GetTokenString(JToken token)
        {
            switch (token.Type)
            {
                case JTokenType.Null:
                case JTokenType.Undefined:
                    return null;

                case JTokenType.String:
                    return (string)token;

                case JTokenType.Integer:
                case JTokenType.Float:
                case JTokenType.Boolean:
                    // For scalars we simply delegate to Json.Net (JsonConvert) for string conversions
                    //  This ensures the string conversion matches the JsonTextWriter
                    return JsonConvert.ToString(((JValue)token).Value);

                case JTokenType.Date:
                case JTokenType.TimeSpan:
                case JTokenType.Guid:
                    // For scalars we simply delegate to Json.Net (JsonConvert) for string conversions
                    //  Note: We want to leverage JsonConvert to ensure the string conversion matches the JsonTextWriter
                    //        However that places surrounding quotes for these data types.
                    var v = JsonConvert.ToString(((JValue)token).Value);
                    return v != null && v.Length > 2 && v[0] == '"' && v[v.Length - 1] == '"' ? v.Substring(1, v.Length - 2) : v;

                default:
                    // For containers we delegate to Json.Net (JToken.ToString/WriteTo) which is capable of serializing all data types, including nested containers
                    return token.ToString();
            }
        }

        /// <summary/>
        internal static object ConvertToken(JToken token, Type type)
        {
            try
            {
                if (type == typeof(string))
                {
                    return JsonFunctions.GetTokenString(token);
                }

                // We simply delegate to Json.Net for data conversions
                return token.ToObject(type);
            }
            catch (Exception e)
            {
                // Make this easier to debug (with field and type context)
                //  Note: We don't expose the actual value to be converted in the error message (since it might be sensitive, information disclosure)
                throw new JsonSerializationException(
                    string.Format(typeof(JsonToken).Namespace + " failed to deserialize '{0}' from '{1}' to '{2}'", token.Path, token.Type.ToString(), type.FullName),
                    e);
            }
        }
    }
}