using System;
using System.IO;
using NUnit.Framework;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestPlatform.CoreUtilities.Extensions;
using MySql.Data.MySqlClient;

namespace Frends.MySql.Tests
{
    /// <summary>
    /// THESE TESTS DO NOT WORK UNLESS YOU INSTALL MySql LOCALLY ON YOUR OWN COMPUTER!
    /// </summary>
    [TestFixture]
    // [Ignore("Cannot be run unless you have a properly configured MySql DB running on your local computer")]
    public class MySqlQueryTests
    {
        private string connectionString = "server=localhost;uid=root;pwd=pw;database=test;";

        Options options = new Options
        {
            TimeoutSeconds = 300
        };

        //[OneTimeSetUp]
        [Test, Order(1)]
        public async Task OneTimeSetUp()
        {
            using (var connection = new MySqlConnection(connectionString))
            {
                await connection.OpenAsync();
                try
                {
                    using (var command = new MySqlCommand("CREATE TABLE IF NOT EXISTS DecimalTest(DecimalValue decimal(38,30))", connection))
                    {
                        await command.ExecuteNonQueryAsync();
                    }
                    using (var command = new MySqlCommand("insert into DecimalTest (DecimalValue) values (1.123456789123456789123456789123)", connection))
                    {
                        await command.ExecuteNonQueryAsync();
                    }
                    using (var command = new MySqlCommand("CREATE TABLE IF NOT EXISTS HodorTest(name varchar(15), value int(10))", connection))
                    {
                        await command.ExecuteNonQueryAsync();
                    }
                    using (var command = new MySqlCommand("insert into HodorTest (name, value) values ('hodor', 123), ('jon', 321);", connection))
                    {
                        await command.ExecuteNonQueryAsync();
                    }
                    using (var command = new MySqlCommand("DROP PROCEDURE IF EXISTS GetAllFromHodorTest; CREATE PROCEDURE GetAllFromHodorTest() BEGIN SELECT * FROM HodorTest; END", connection))
                    {
                        await command.ExecuteNonQueryAsync();
                    }
                }
                catch (Exception)
                {
                    // table probably/procedure exist already
                    throw;
                }
            }
        }

        //[OneTimeTearDown]
        [Test, Order(50)]
        public async Task OneTimeTearDown()
        {
            using (var connection = new MySqlConnection(connectionString))
            {
                await connection.OpenAsync();

                using (var command = new MySqlCommand("drop table HodorTest", connection))
                {
                    await command.ExecuteNonQueryAsync();
                }
                using (var command = new MySqlCommand("drop table DecimalTest", connection))
                {
                    await command.ExecuteNonQueryAsync();
                }
                using (var command = new MySqlCommand("DROP PROCEDURE GetAllFromHodorTest;", connection))
                {
                    await command.ExecuteNonQueryAsync();
                }
            }
        }



        [Test, Order(2)]
        public async Task ShouldSuccess_DoBasicQuery()
        {
            var q = new QueryInput
            {
                ConnectionString = connectionString,
                CommandText = @"select  * from hodortest limit 2"

            };

            options.MySqlTransactionIsolationLevel = MySqlTransactionIsolationLevel.Default;

            var result = await QueryTask.ExecuteQuery(q, options, new CancellationToken());

            Assert.That(result.ToString(), Is.EqualTo(@"[
  {
    ""name"": ""hodor"",
    ""value"": 123
  },
  {
    ""name"": ""jon"",
    ""value"": 321
  }
]"));

        }

        [Test, Order(3)]
        public void ShouldThrowException_DoBasicQuery()
        {
            var q = new QueryInput
            {
                ConnectionString = connectionString,
                CommandText = @"select  * from tablex limit 2"
            };


            options.MySqlTransactionIsolationLevel = MySqlTransactionIsolationLevel.Default;

            Exception ex = Assert.ThrowsAsync<Exception>(() => QueryTask.ExecuteQuery(q, options, new CancellationToken()));
            Assert.That(ex.Message.StartsWith("Query failed"));
        }



        [Test, Order(4)]
        public async Task ShouldSuccess_CallStoredProcedure()
        {
            var q = new QueryInput
            {
                ConnectionString = connectionString,
                CommandText = @"GetAllFromHodorTest"

            };

            options.MySqlTransactionIsolationLevel = MySqlTransactionIsolationLevel.Default;

           var result = await QueryTask.ExecuteProcedure(q, options, new CancellationToken());
        }
        [Test, Order(5)]
        public void ShouldThrowException_CallStoredProcedure()
        {
            var q = new QueryInput
            {
                ConnectionString = connectionString,
                CommandText = @"GetAllFromHodorTest00"

            };

            options.MySqlTransactionIsolationLevel = MySqlTransactionIsolationLevel.Default;

            Exception ex = Assert.ThrowsAsync<Exception>(() => QueryTask.ExecuteQuery(q, options, new CancellationToken()));
            Assert.That(ex.Message.StartsWith("Query failed"));

        }



        [Test, Order(6)]
        public async Task ShouldSuccess_InsertValues()
        {

            string rndName = Path.GetRandomFileName();
            Random rnd = new Random();
            int rndValue = rnd.Next(1000);
            var q = new QueryInput
            {
                ConnectionString = connectionString,
                CommandText = "insert into HodorTest (name, value) values ( " + rndName.AddDoubleQuote() + " , " + rndValue + " );"

            };
            options.MySqlTransactionIsolationLevel = MySqlTransactionIsolationLevel.Default;

            var result = await QueryTask.ExecuteQuery(q, options, new CancellationToken());

            Assert.That(result.ToString().Equals("1"));

        }
        public static string AddDoubleQuotes(string value)
        {
            return "\"" + value + "\"";
        }


        [Test, Order(7)]
        public async Task ShouldSuccess_DoBasicQueryOneValue()
        {
            var q = new QueryInput
            {
                ConnectionString = connectionString,
                CommandText = "SELECT value FROM HodorTest WHERE name LIKE 'hodor' limit 1 "


            };

            options.MySqlTransactionIsolationLevel = MySqlTransactionIsolationLevel.Default;

            var result = await QueryTask.ExecuteQuery(q, options, new CancellationToken());

            Assert.That(result.ToString(), Is.EqualTo(@"[
  {
    ""value"": 123
  }
]"));

        }

        [Test, Order(8)]
        public void ShouldThrowException_FaultyConnectionString()
        {
            var q = new QueryInput
            {
                ConnectionString = connectionString + "nonsense",
                CommandText = "SELECT value FROM HodorTest WHERE name LIKE 'hodor' limit 1 "

            };

            options.MySqlTransactionIsolationLevel = MySqlTransactionIsolationLevel.Default;

            Exception ex = Assert.ThrowsAsync<Exception>(() => QueryTask.ExecuteQuery(q, options, new CancellationToken()));
            Assert.That(ex.Message.StartsWith("Format of the initialization string"));

        }
        [Test, Order(9)]
        public void ShouldThrowException_CancellationRequested()
        {
            var q = new QueryInput
            {
                ConnectionString = connectionString + "nonsense",
                CommandText = "SELECT value FROM HodorTest WHERE name LIKE 'hodor' limit 1 "

            };

            options.MySqlTransactionIsolationLevel = MySqlTransactionIsolationLevel.Default;

            Exception ex = Assert.ThrowsAsync<TaskCanceledException>(() => QueryTask.ExecuteQuery(q, options, new CancellationToken(true)));
            Assert.That(ex.Message.StartsWith("A task was canceled"));

        }

    }
}
