Rsqldrv is the C&#35; driver to communicate with RSQL database server (http://rsql.ch).

The driver source code is free software (Apache license).


### Driver Description

This driver contains the SqlConnection, SqlCommand, SqlDataReader, SqlParameter and SqlTransaction objects.

They are used the same way as the driver for MS SQL driver.

You can read the Microsoft documentation about these objects, as they implement all needed methods and properties (e.g. SqlConnection: https://msdn.microsoft.com/en-us/library/system.data.sqlclient.sqlconnection(v=vs.110).aspx))

Methods for asynchronous programming are not implemented, though.


### Installation

To install the driver for RSQL:
- Download the library dll from http://rsql.ch/guide/download_page/.
- Install the dll file in a `lib` (or any other name) directory under your project directory.
- In the "Project/Add Reference" menu of Visual Studio, add this library.


### Sample Program

Below is a sample program that inserts records into a table and reads them.
- The line `using System.Data.SqlClient;` references the driver for MS SQL Server.
- To use the driver for RSQL, just replace it by the line `using Rsqldrv.SqlClient;`, as in the code below.

You can develop your application on RSQL, and if you want to switch to and from MS SQL Server, you just need to change the `using` line.


```C#
using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.Common;
using System.Data.SqlTypes;

//using System.Data.SqlClient;
using Rsqldrv.SqlClient;

namespace Sample
{
    class Program
    {
        static void Main(string[] args)
        {
            string connString = "user id=sa; password=changeme; server=127.0.0.1";

            using (SqlConnection connection = new SqlConnection(connString))
            {
                try
                {
                    connection.Open();

                    truncateTable(connection); // truncate table

                    insertRowsInsideTransaction(connection); // insert rows inside a transaction

                    insertRowsWithParameters(connection); // insert rows with parameters

                    readRows(connection); // print all rows

                    readRowCount(connection); // print row count
                }
                catch (Exception ex)
                {
                    //Console.WriteLine("Exception Type: {0}", ex.GetType());
                    //Console.WriteLine("  Message: {0}", ex.Message);
                    Console.WriteLine(ex);
                }

                connection.Close(); // in fact, not needed, as the "using" clause will dispose (that is, close) the connection
            }

            Console.WriteLine("Please press any key to exit...");
            Console.ReadKey();
        }

        // truncate table
        static void truncateTable(SqlConnection connection)
        {
            using (SqlCommand command = connection.CreateCommand())
            {
                command.CommandText = String.Format("TRUNCATE TABLE mytest.dbo.mytable;");
                command.ExecuteNonQuery();

                Console.WriteLine("Table has been truncated.");
            }
        }

        // insert rows into a table inside transaction
        static void insertRowsInsideTransaction(SqlConnection connection)
        {
            using (SqlCommand command = connection.CreateCommand())
            {
                SqlTransaction transaction = connection.BeginTransaction();
                command.Connection = connection;
                command.Transaction = transaction;

                try
                {
                    for (int i = 100; i < 110; i++)
                    {
                        command.CommandText = String.Format("INSERT INTO mytest.dbo.mytable (id, name) VALUES ({0}, 'hello_{1}')", i, i);
                        command.ExecuteNonQuery();
                    }

                    transaction.Commit();
                    Console.WriteLine("All records have been written to database inside a transaction.");
                }
                catch (Exception ex)
                {
                    try { transaction.Rollback(); } // rollback the transaction
                    catch { } // silently ignore any error raised by rollback.

                    throw ex; // rethrow exception
                }
            }
        }

        // insert rows into a table with parameters
        static void insertRowsWithParameters(SqlConnection connection)
        {
            using (SqlCommand command = connection.CreateCommand())
            {
                command.CommandText = String.Format("INSERT INTO mytest.dbo.mytable (id, name) VALUES (@id, @name)");

                SqlParameter pId = new SqlParameter("@id", SqlDbType.Int);
                command.Parameters.Add(pId);
                SqlParameter pName = new SqlParameter("@name", SqlDbType.VarChar);
                command.Parameters.Add(pName);

                pId.Value = 1000;
                pName.Value = "John";
                command.ExecuteNonQuery();

                pId.Value = 1001;
                pName.Value = "Peter";
                command.ExecuteNonQuery();

                Console.WriteLine("All records have been written to database, using a command with parameters.");
            }
        }

        // read rows from a table
        static void readRows(SqlConnection connection)
        {
            using (SqlCommand command = connection.CreateCommand())
            {
                SqlDataReader reader = null;

                try
                {
                    command.CommandText = "SELECT id, name FROM mytest.dbo.mytable ORDER BY id;";
                    reader = command.ExecuteReader();

                    while (reader.Read())
                    {
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            Console.Write("{0}\t", reader[i].ToString());
                        }
                        Console.WriteLine();
                    }
                }
                finally
                {
                    if (reader != null)
                    {
                        reader.Dispose();
                    }
                }
            }
        }

        // read row count
        static void readRowCount(SqlConnection connection)
        {
            using (SqlCommand command = connection.CreateCommand())
            {
                command.CommandText = "SELECT count(*) FROM mytest.dbo.mytable;";
                int count = (int)command.ExecuteScalar();

                Console.WriteLine("Record count: {0}", count);
            }
        }


    }
}

```

