using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Dynamic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using MySql.Data.MySqlClient;
using Newtonsoft.Json.Linq;

namespace Frends.Community.MySql
{
    /// <summary>
    /// Tasks for performing MySql queries.
    /// </summary>
    public static class MySqlTasks
    {


        /// <summary>
        ///  Execute a sql query. See documentation at https://github.com/CommunityHiQ/Frends.Community.MySQL#executequery
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
        /// Execute a stored procedure. See documentation at https://github.com/CommunityHiQ/Frends.Community.MySQL#ExecuteProcedure
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

        [SuppressMessage("Security",
            "CA2100:Review SQL queries for security vulnerabilities", Justification =
                "One is able to write queries in FRENDS. It is up to a FRENDS process prevent injections.")]
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
                                    throw new Exception("Query failed " + ex.Message);

                                }

                            }
                        }

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
                                throw new Exception("Query failed " + ex.Message);

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

