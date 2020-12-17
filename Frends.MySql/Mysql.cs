using Dapper;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Dynamic;
using System.Runtime.CompilerServices;

namespace Frends.MySql
{
    /// <summary>
    /// Example task package for handling files
    /// </summary>
    public static class QueryTask
    {


        /// <summary>
        /// Task for performing queries in Oracle databases. See documentation at https://github.com/CommunityHiQ/Frends.Community.Oracle.Query
        /// </summary>
        /// <param name="query"></param>
        /// <param name="options"></param>
        /// <param name="cancellationToken"></param>
        /// <returns>Object { bool Success, string Message, JToken Result }</returns>
        public static async Task<JToken> ExecuteQuery(
            [PropertyTab] QueryInput query,
            [PropertyTab] Options options,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return await GetMySqlCommandResult(query.CommandText, query.ConnectionString, query.Parameters, options,
                CommandType.Text, cancellationToken);
        }

        /// <summary>
        /// Task for performing queries in Oracle databases. See documentation at https://github.com/CommunityHiQ/Frends.Community.Oracle.Query
        /// </summary>
        /// <param name="query"></param>
        /// <param name="options"></param>
        /// <param name="cancellationToken"></param>
        /// <returns>Object { bool Success, string Message, JToken Result }</returns>
        public static async Task<JToken> ExecuteProcedure(
            [PropertyTab] QueryInput query,
            [PropertyTab] Options options,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return await GetMySqlCommandResult(query.CommandText, query.ConnectionString, query.Parameters, options,
                CommandType.StoredProcedure, cancellationToken);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Security",
            "CA2100:Review SQL queries for security vulnerabilities", Justification =
                "One is able to write quereis in FRENDS. It is up to a FRENDS process prevent injections.")]
        private static async Task<JToken> GetMySqlCommandResult(
            string query, string connectionString, IEnumerable<Parameter> parameters,
            Options options,
            CommandType commandType,
            CancellationToken cancellationToken)
        {

            var scalarReturnQueries = new[] { "update ", "insert ", "drop ", "truncate ", "create ", "alter " };

            try
            {
                using (var conn = new MySqlConnection(connectionString))
                {
                    await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
                    cancellationToken.ThrowIfCancellationRequested();

                    IDictionary<string, object> parameterObject = new ExpandoObject();
                    if (parameters != null)
                    {
                        foreach (var parameter in parameters)
                        {
                            parameterObject.Add(parameter.Name, parameter.Value);
                        }

                    }

                    using (var command = new MySqlCommand(query, conn))
                    {
                        command.CommandTimeout = options.TimeoutSeconds;
                        command.CommandType = commandType;

                        IsolationLevel isolationLevel;
                        switch (options.MySqlTransactionIsolationLevel)
                        {
                            case MySqlTransactionIsolationLevel.ReadCommitted:
                                isolationLevel = IsolationLevel.ReadCommitted;
                                break;
                            case MySqlTransactionIsolationLevel.ReadUncommitted:
                                isolationLevel = IsolationLevel.ReadUncommitted;
                                break;
                            case MySqlTransactionIsolationLevel.RepeatableRead:
                                isolationLevel = IsolationLevel.RepeatableRead;
                                break;
                            case MySqlTransactionIsolationLevel.Serializable:
                                isolationLevel = IsolationLevel.Serializable;
                                break;
                            default:
                                isolationLevel = IsolationLevel.RepeatableRead;
                                break;
                        }


                        if (scalarReturnQueries.Any(query.TrimStart().ToLower().Contains) || command.CommandType == CommandType.StoredProcedure)
                        {
                            // scalar return
                            using (var trans = conn.BeginTransaction(isolationLevel))
                            {
                                try
                                {
                                    var affectedRows = await conn.ExecuteAsync(query, parameterObject, trans,
                                        command.CommandTimeout, command.CommandType);

                                    trans.Commit();

                                    return JToken.FromObject(affectedRows);

                                }
                                catch (Exception ex)
                                {
                                    trans.Rollback();
                                    trans.Dispose();
                                    throw new Exception($"Query failed " + ex.Message);

                                }

                            }
                        }
                        else
                        {
                            using (var trans = conn.BeginTransaction(isolationLevel))
                            {
                                try
                                {
                                    var result = await conn.QueryAsync(query, parameterObject, trans, command.CommandTimeout, command.CommandType)
                                        .ConfigureAwait(false);

                                    trans.Commit();

                                    return JToken.FromObject(result);
                                }
                                catch (Exception ex)
                                {
                                    trans.Rollback();
                                    trans.Dispose();
                                    throw new Exception($"Query failed " + ex.Message);

                                }

                            }
                        }

                    }

                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }


        }


    }


}

