﻿using NUnit.Framework;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using MySql;
using MySql.Data;
using MySql.Data.MySqlClient;
using Frends.Community.MySql;

namespace Frends.Community.MySql.Tests
{
    /// <summary>
    /// THESE TESTS DO NOT WORK UNLESS YOU INSTALL MySql LOCALLY ON YOUR OWN COMPUTER!
    /// </summary>
    [TestFixture]
    [Ignore("Cannot be run unless you have a properly configured MySql DB running on your local computer")]
    public class MySqlQueryTests
    {
        // Problems with local MySql, tests not implemented yet

        ConnectionProperties _conn = new ConnectionProperties
        {
            ConnectionString = "server=localhost;uid=SYSTEM;pwd=<<your password>>;database=test;",
            TimeoutSeconds = 300
        };

        [OneTimeSetUp]
        public async Task OneTimeSetUp()
        {
            using (var connection = new MySqlConnection(_conn.ConnectionString))
            {
                await connection.OpenAsync();

                using (var command = new MySqlCommand("create table DecimalTest(DecimalValue decimal(38,35))", connection))
                {
                    await command.ExecuteNonQueryAsync();
                }
                using (var command = new MySqlCommand("insert into DecimalTest (DecimalValue) values (1.12345678912345678912345678912345678)", connection))
                {
                    await command.ExecuteNonQueryAsync();
                }
                using (var command = new MySqlCommand("create table HodorTest(name varchar2(15), value number(10,0))", connection))
                {
                    await command.ExecuteNonQueryAsync();
                }
                using (var command = new MySqlCommand("insert all into HodorTest values('hodor', 123) into HodorTest values('jon', 321) select 1 from dual", connection))
                {
                    await command.ExecuteNonQueryAsync();
                }
            }
        }

        [OneTimeTearDown]
        public async Task OneTimeTearDown()
        {
            using (var connection = new MySqlConnection(_conn.ConnectionString))
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
            }
        }

        [Test]
        [Category("Xml tests")]
        public async Task ShouldReturnXmlString()
        {
            var q = new QueryProperties { Query = @"select * from HodorTest" };
            var o = new QueryOutputProperties
            {
                ReturnType = QueryReturnType.Xml,
                XmlOutput = new XmlOutputProperties
                {
                    RootElementName = "items",
                    RowElementName = "item"
                }
            };
            var options = new Options { ThrowErrorOnFailure = true };

            Output result = await MySql.Query(q, o, _conn, options, new CancellationToken());

            Assert.AreEqual(@"<?xml version=""1.0"" encoding=""utf-16""?>
<items>
  <item>
    <NAME>hodor</NAME>
    <VALUE>123</VALUE>
  </item>
  <item>
    <NAME>jon</NAME>
    <VALUE>321</VALUE>
  </item>
</items>", result.Result);
        }

        [Test]
        [Category("Xml tests")]
        public async Task ShouldWriteXmlFile()
        {
            var q = new QueryProperties { Query = @"select name as ""name"", value as ""value"" from HodorTest" };
            var o = new QueryOutputProperties
            {
                ReturnType = QueryReturnType.Xml,
                XmlOutput = new XmlOutputProperties
                {
                    RootElementName = "items",
                    RowElementName = "item"
                },
                OutputToFile = true,
                OutputFile = new OutputFileProperties
                {
                    Path = Path.Combine(Directory.GetCurrentDirectory(), Guid.NewGuid().ToString() + ".xml")
                }
            };
            var options = new Options { ThrowErrorOnFailure = true };

            Output result = await MySql.Query(q, o, _conn, options, new CancellationToken());

            Assert.IsTrue(File.Exists(result.Result), "should have created xml output file");
            Assert.AreEqual(
                @"<?xml version=""1.0"" encoding=""utf-8""?>
<items>
  <item>
    <name>hodor</name>
    <value>123</value>
  </item>
  <item>
    <name>jon</name>
    <value>321</value>
  </item>
</items>",
                File.ReadAllText(result.Result));
            File.Delete(result.Result);
        }

        /// <summary>
        /// A simple query that fetches a decimal value from the database
        /// </summary>
        [Test]
        [Category("Json tests")]
        public async Task QueryDatabaseJSON()
        {
            var queryProperties = new QueryProperties { Query = "SELECT * FROM DecimalTest" };
            var outputProperties = new QueryOutputProperties
            {
                ReturnType = QueryReturnType.Json,
                JsonOutput = new JsonOutputProperties()
            };
            var options = new Options { ThrowErrorOnFailure = true };

            Output result = await MySql.Query(queryProperties, outputProperties, _conn, options, new CancellationToken());

            Assert.AreNotEqual("", result.Result);
            Assert.AreEqual(true, result.Success);
        }

        [Test]
        [Category("Json tests")]
        public async Task ShouldReturnJsonString()
        {
            var q = new QueryProperties { Query = @"select name as ""name"", value as ""value"" from HodorTest" };
            var o = new QueryOutputProperties
            {
                ReturnType = QueryReturnType.Json,
                JsonOutput = new JsonOutputProperties(),
                OutputToFile = false
            };
            var options = new Options { ThrowErrorOnFailure = true };

            Output result = await MySql.Query(q, o, _conn, options, new CancellationToken());

            Assert.IsTrue(string.Equals(result.Result, @"[
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

        [Test]
        [Category("Json tests")]
        public async Task ShouldWriteJsonFile()
        {
            var q = new QueryProperties { Query = @"select name as ""name"", value as ""value"" from HodorTest" };
            var o = new QueryOutputProperties
            {
                ReturnType = QueryReturnType.Json,
                JsonOutput = new JsonOutputProperties(),
                OutputToFile = true,
                OutputFile = new OutputFileProperties
                {
                    Path = Path.Combine(Directory.GetCurrentDirectory(), Guid.NewGuid().ToString() + ".json")
                }
            };
            var options = new Options { ThrowErrorOnFailure = true };

            Output result = await MySql.Query(q, o, _conn, options, new CancellationToken());

            Assert.IsTrue(File.Exists(result.Result), "should have created json outputfile");
            Assert.AreEqual(@"[
  {
    ""name"": ""hodor"",
    ""value"": 123
  },
  {
    ""name"": ""jon"",
    ""value"": 321
  }
]",
                File.ReadAllText(result.Result));
            File.Delete(result.Result);
        }

        [Test]
        [Category("Csv tests")]
        public async Task ShouldReturnCsvString()
        {
            var q = new QueryProperties { Query = @"select name as ""name"", value as ""value"" from HodorTest" };
            var o = new QueryOutputProperties
            {
                ReturnType = QueryReturnType.Csv,
                CsvOutput = new CsvOutputProperties
                {
                    CsvSeparator = ";",
                    IncludeHeaders = true
                }
            };
            var options = new Options { ThrowErrorOnFailure = true };

            Output result = await MySql.Query(q, o, _conn, options, new CancellationToken());

            StringAssert.IsMatch(result.Result, "name;value\r\nhodor;123\r\njon;321\r\n");
        }

        [Test]
        [Category("Csv tests")]
        public async Task ShouldWriteCsvFile()
        {
            var q = new QueryProperties { Query = "select * from HodorTest" };
            var o = new QueryOutputProperties
            {
                ReturnType = QueryReturnType.Csv,
                CsvOutput = new CsvOutputProperties
                {
                    CsvSeparator = ";",
                    IncludeHeaders = true
                },
                OutputToFile = true,
                OutputFile = new OutputFileProperties
                {
                    Path = Path.Combine(Directory.GetCurrentDirectory(), Guid.NewGuid().ToString() + ".csv")
                }
            };
            var options = new Options { ThrowErrorOnFailure = true };

            Output result = await MySql.Query(q, o, _conn, options, new CancellationToken());

            Assert.IsTrue(File.Exists(result.Result), "should have created csv output file");
            File.Delete(result.Result);
        }
    }
}
