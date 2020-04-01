using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Data;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using MySql;
using MySql.Data;
using MySql.Data.MySqlClient;
using MySql.Data.Types;

namespace Frends.Community.MySql
{
    static class Extensions
    {
        public static TEnum ConvertEnum<TEnum>(this Enum source)
        {
            return (TEnum)Enum.Parse(typeof(TEnum), source.ToString(), true);
        }

        /// <summary>
        /// Write query results to csv string or file
        /// </summary>
        /// <param name="command"></param>
        /// <param name="output"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static async Task<string> ToXmlAsync(this MySqlCommand command, QueryOutputProperties output, CancellationToken cancellationToken)
        {
            command.CommandType = CommandType.Text;
            
            // utf-8 as default encoding
            Encoding encoding = string.IsNullOrWhiteSpace(output.OutputFile?.Encoding) ? Encoding.UTF8 : Encoding.GetEncoding(output.OutputFile.Encoding);

            using (TextWriter writer = output.OutputToFile ? new StreamWriter(output.OutputFile.Path, false, encoding) : new StringWriter() as TextWriter)
            using (MySqlDataReader reader = await command.ExecuteReaderAsync(cancellationToken) as MySqlDataReader)
            {
                using (XmlWriter xmlWriter = XmlWriter.Create(writer, new XmlWriterSettings { Async = true, Indent = true }))
                {
                    await xmlWriter.WriteStartDocumentAsync();
                    await xmlWriter.WriteStartElementAsync("", output.XmlOutput.RootElementName, "");

                    while (await reader.ReadAsync(cancellationToken))
                    {
                        // single row element container
                        await xmlWriter.WriteStartElementAsync("", output.XmlOutput.RowElementName, "");

                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            await xmlWriter.WriteElementStringAsync("", reader.GetName(i), "", reader.GetValue(i).ToString());
                        }

                        // close single row element container
                        await xmlWriter.WriteEndElementAsync();

                        // write only complete elements, but stop if process was terminated
                        cancellationToken.ThrowIfCancellationRequested();
                    }

                    await xmlWriter.WriteEndElementAsync();
                    await xmlWriter.WriteEndDocumentAsync();
                }

                if (output.OutputToFile)
                {
                    return output.OutputFile.Path;
                }
                else
                {
                    return writer.ToString();
                }
            }
        }

        /// <summary>
        /// Write query results to json string or file
        /// </summary>
        /// <param name="command"></param>
        /// <param name="output"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static async Task<string> ToJsonAsync(this MySqlCommand command, QueryOutputProperties output, CancellationToken cancellationToken)
        {
            command.CommandType = CommandType.Text;
            
            using (MySqlDataReader reader = await command.ExecuteReaderAsync(cancellationToken) as MySqlDataReader)
            {
                var culture = String.IsNullOrWhiteSpace(output.JsonOutput.CultureInfo) ? CultureInfo.InvariantCulture : new CultureInfo(output.JsonOutput.CultureInfo);

                // utf-8 as default encoding
                Encoding encoding = string.IsNullOrWhiteSpace(output.OutputFile?.Encoding) ? Encoding.UTF8 : Encoding.GetEncoding(output.OutputFile.Encoding);

                // create json result
                using (var fileWriter = output.OutputToFile ? new StreamWriter(output.OutputFile.Path, false, encoding) : null)
                using (var writer = output.OutputToFile ? new JsonTextWriter(fileWriter) : new JTokenWriter() as JsonWriter)
                {
                    writer.Formatting = Newtonsoft.Json.Formatting.Indented;
                    writer.Culture = culture;

                    // start array
                    await writer.WriteStartArrayAsync(cancellationToken);

                    cancellationToken.ThrowIfCancellationRequested();

                    while (reader.Read())
                    {
                        // start row object
                        await writer.WriteStartObjectAsync(cancellationToken);

                        for (var i = 0; i < reader.FieldCount; i++)
                        {
                            // add row element name
                            await writer.WritePropertyNameAsync(reader.GetName(i), cancellationToken);
                            
                            await writer.WriteValueAsync(reader.GetValue(i) ?? string.Empty, cancellationToken);

                            cancellationToken.ThrowIfCancellationRequested();
                        }

                        await writer.WriteEndObjectAsync(cancellationToken); // end row object

                        cancellationToken.ThrowIfCancellationRequested();
                    }

                    // end array
                    await writer.WriteEndArrayAsync(cancellationToken);

                    if (output.OutputToFile)
                    {
                        return output.OutputFile.Path;
                    }
                    else
                    {
                        return ((JTokenWriter)writer).Token.ToString();
                    }
                }
            }
        }

        /// <summary>
        /// Write query results to csv string or file
        /// </summary>
        /// <param name="command"></param>
        /// <param name="output"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static async Task<string> ToCsvAsync(this MySqlCommand command, QueryOutputProperties output, CancellationToken cancellationToken)
        {
            command.CommandType = CommandType.Text;

            // utf-8 as default encoding
            Encoding encoding = string.IsNullOrWhiteSpace(output.OutputFile?.Encoding) ? Encoding.UTF8 : Encoding.GetEncoding(output.OutputFile.Encoding);

            using (MySqlDataReader reader = await command.ExecuteReaderAsync(cancellationToken) as MySqlDataReader)
            using (TextWriter w = output.OutputToFile ? new StreamWriter(output.OutputFile.Path, false, encoding) : new StringWriter() as TextWriter)
            {
                bool headerWritten = false;

                while (await reader.ReadAsync(cancellationToken))
                {
                    // write csv header if necessary
                    if (!headerWritten && output.CsvOutput.IncludeHeaders)
                    {
                        var fieldNames = new object[reader.FieldCount];
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            fieldNames[i] = reader.GetName(i);
                        }
                        await w.WriteLineAsync(string.Join(output.CsvOutput.CsvSeparator, fieldNames));
                        headerWritten = true;
                    }

                    var fieldValues = new object[reader.FieldCount];
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        fieldValues[i] = reader.GetValue(i);
                    }
                    await w.WriteLineAsync(string.Join(output.CsvOutput.CsvSeparator, fieldValues));

                    // write only complete rows, but stop if process was terminated
                    cancellationToken.ThrowIfCancellationRequested();
                }

                if (output.OutputToFile)
                {
                    return output.OutputFile.Path;
                }
                else
                {
                    return w.ToString();
                }
            }
        }
    }
}
