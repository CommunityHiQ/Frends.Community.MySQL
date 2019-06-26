using Newtonsoft.Json;
using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using System.ComponentModel;
using MySql;
using MySql.Data;
using MySql.Data.MySqlClient;

namespace Frends.Community.MySql
{
    /// <summary>
    /// Task for performing queries in MySql databases. See documentation at https://github.com/CommunityHiQ/Frends.Community.MySQL
    /// </summary>
    public class MySql
    {
        /// <summary>
        /// Task for performing queries in MySql databases. See documentation at https://github.com/CommunityHiQ/Frends.Community.MySQL
        /// </summary>
        /// <param name="query"></param>
        /// <param name="output"></param>
        /// <param name="connection"></param>
        /// <param name="options"></param>
        /// <param name="cancellationToken"></param>
        /// <returns>Object { bool Success, string Message, string Result }</returns>
        public static async Task<Output> Query(
            [PropertyTab] QueryProperties query,
            [PropertyTab] QueryOutputProperties output,
            [PropertyTab] ConnectionProperties connection,
            [PropertyTab] Options options,
            CancellationToken cancellationToken)
        {
            try
            {
                using (var c = new MySqlConnection(connection.ConnectionString))
                {
                    try
                    {
                        await c.OpenAsync(cancellationToken);

                        using (var command = new MySqlCommand(query.Query, c))
                        {
                            command.CommandTimeout = connection.TimeoutSeconds;
                            
                            // check for command parameters and set them
                            if (query.Parameters != null)
                                command.Parameters.AddRange(query.Parameters.Select(p => CreateMySqlQueryParameter(p)).ToArray());

                            // declare Result object
                            string queryResult;

                            // set commandType according to ReturnType
                            switch (output.ReturnType)
                            {
                                case QueryReturnType.Xml:
                                    queryResult = await command.ToXmlAsync(output, cancellationToken);
                                    break;
                                case QueryReturnType.Json:
                                    queryResult = await command.ToJsonAsync(output, cancellationToken);
                                    break;
                                case QueryReturnType.Csv:
                                    queryResult = await command.ToCsvAsync(output, cancellationToken);
                                    break;
                                default:
                                    throw new ArgumentException("Task 'Return Type' was invalid! Check task properties.");
                            }

                            return new Output { Success = true, Result = queryResult };
                        }
                    }
                    catch (Exception ex)
                    {
                        throw ex;
                    }
                    finally
                    {
                        // Close connection
                        c.Dispose();
                        c.Close();
                        MySqlConnection.ClearPool(c);
                    }
                }
            }
            catch (Exception ex)
            {
                if (options.ThrowErrorOnFailure)
                    throw ex;
                return new Output
                {
                    Success = false,
                    Message = ex.Message
                };
            }
        }
        /// <summary>
        /// Gets list of parameters available for MySql Query.
        /// </summary>
        public static MySqlParameter CreateMySqlQueryParameter(QueryParameter parameter)
        {
            return new MySqlParameter()
            {
                ParameterName = parameter.Name,
                Value = parameter.Value,
                MySqlDbType = parameter.DataType.ConvertEnum<MySqlDbType>()
            };
        }

    }
}
