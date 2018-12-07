using System;
using System.Data.SqlClient;
using System.Text;

namespace Program
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                // Build connection string.
                SqlConnectionStringBuilder connectionStringBuilder = new SqlConnectionStringBuilder
                {
                    DataSource = "172.30.95.206",
                    UserID = "sa",
                    Password = "SSlx&gw7!",
                    InitialCatalog = "master"
                };

                // Connect to SQL Server.
                Console.Write("Connecting to SQL Server ... ");
                using (SqlConnection connection = new SqlConnection(connectionStringBuilder.ConnectionString))
                {
                    connection.Open();
                    Console.WriteLine("Done.");

                    // Demo 1.
                    Console.WriteLine("*** Create, Insert, Update, Delete and Select operations demo. ***");

                    // Create a database.
                    Console.Write("Dropping and creating database 'SampleDB' ... ");
                    string sql = "DROP DATABASE IF EXISTS [SampleDB]; CREATE DATABASE [SampleDB]";
                    using (SqlCommand command = new SqlCommand(sql, connection))
                    {
                        command.ExecuteNonQuery();
                        Console.WriteLine("Done.");
                    }

                    // Create a table and insert some data.
                    Console.Write("Creating table 'Emplyees' with data, press any key to continue ... ");
                    Console.ReadKey(true);
                    StringBuilder builder = new StringBuilder();
                    builder.Append("USE SampleDB; ");
                    builder.Append("CREATE TABLE Employees ( ");
                    builder.Append(" Id INT IDENTITY(1,1) NOT NULL PRIMARY KEY, ");
                    builder.Append(" Name NVARCHAR(50), ");
                    builder.Append(" Location NVARCHAR(50) ");
                    builder.Append("); ");
                    builder.Append("INSERT INTO Employees (Name, Location) VALUES ");
                    builder.Append("(N'LiuBei', N'ZhuoZhou'), ");
                    builder.Append("(N'GuanYu', N'ChangZhou'), ");
                    builder.Append("(N'ZhangFei', N'YouZhou'); ");
                    sql = builder.ToString();
                    using (SqlCommand command = new SqlCommand(sql, connection))
                    {
                        command.ExecuteNonQuery();
                        Console.WriteLine("Done.");
                    }

                    // Insert a row into table.
                    Console.Write("Inserting a new row into table, press any key to continue ... ");
                    Console.ReadKey(true);
                    builder.Length = 0;
                    builder.Capacity = 0;
                    builder.Append("INSERT Employees (Name, Location) ");
                    builder.Append("VALUES (@name, @location);");
                    sql = builder.ToString();
                    using (SqlCommand command = new SqlCommand(sql, connection))
                    {
                        command.Parameters.AddWithValue("@name", "ZhuGeLiang");
                        command.Parameters.AddWithValue("@Location", "JingZhou");
                        int rowsAffected = command.ExecuteNonQuery();
                        Console.WriteLine(rowsAffected + " row(s) inserted.");
                    }

                    // Update table.
                    string userToUpdate = "GuanYu";
                    Console.Write("Updating 'Location' for user '" + userToUpdate + "', press any key to continue ... ");
                    Console.ReadKey(true);
                    builder.Length = 0;
                    builder.Capacity = 0;
                    builder.Append("UPDATE Employees SET Location = N'QingZhou' WHERE Name = @name");
                    sql = builder.ToString();
                    using (SqlCommand command = new SqlCommand(sql, connection))
                    {
                        command.Parameters.AddWithValue("@name", userToUpdate);
                        int rowsAffected = command.ExecuteNonQuery();
                        Console.WriteLine(rowsAffected + " row(s) updated.");
                    }

                    // Delete table.
                    string userToDelete = "ZhangFei";
                    Console.Write("Deleting user '" + userToDelete + "', press any key to continue ... ");
                    Console.ReadKey(true);
                    builder.Length = 0;
                    builder.Capacity = 0;
                    builder.Append("DELETE FROM Employees WHERE Name = @name;");
                    sql = builder.ToString();
                    using (SqlCommand command = new SqlCommand(sql, connection))
                    {
                        command.Parameters.AddWithValue("@name", userToDelete);
                        int rowAffected = command.ExecuteNonQuery();
                        Console.WriteLine(rowAffected + " row(s) deleted.");
                    }

                    // Read data from table.
                    Console.WriteLine("Reading data from table, press any key to continue ... ");
                    Console.ReadKey(true);
                    builder.Length = 0;
                    builder.Capacity = 0;
                    builder.Append("SELECT Id, Name, Location FROM Employees;");
                    sql = builder.ToString();
                    using (SqlCommand command = new SqlCommand(sql, connection))
                    {
                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                Console.WriteLine("{0} {1} {2}", reader.GetInt32(0), reader.GetString(1), reader.GetString(2));
                            }
                        }
                    }

                    // Demo 2.
                    Console.WriteLine("*** Columnsstore demo. ***");

                    // Insert 5 million rows into the table 'Table_with_5M_rows'.
                    Console.Write("Inserting 5 million rows into table 'Table_with_5M_rows'. This takes ~1 minute, please wait ... ");
                    builder.Length = 0;
                    builder.Capacity = 0;
                    builder.Append("WITH a AS (SELECT * FROM (VALUES(1),(2),(3),(4),(5),(6),(7),(8),(9),(10)) AS a(a))");
                    builder.Append("SELECT TOP(5000000)");
                    builder.Append("ROW_NUMBER() OVER (ORDER BY a.a) AS OrderItemId ");
                    builder.Append(",a.a + b.a + c.a + d.a + e.a + f.a + g.a + h.a AS OrderId ");
                    builder.Append(",a.a * 10 AS Price ");
                    builder.Append(",CONCAT(a.a, N' ', b.a, N' ', c.a, N' ', d.a, N' ', e.a, N' ', f.a, N' ', g.a, N' ', h.a) AS ProductName ");
                    builder.Append("INTO Table_with_5M_rows ");
                    builder.Append("FROM a, a AS b, a AS c, a AS d, a AS e, a AS f, a AS g, a AS h;");
                    sql = builder.ToString();
                    using (SqlCommand command = new SqlCommand(sql, connection))
                    {
                        command.ExecuteNonQuery();
                        Console.WriteLine("Done.");
                    }

                    // Execute SQL query without columnstore index.
                    double elapsedTimeWithoutIndex = SumPrice(connection);
                    Console.WriteLine("Query time WITHOUT columnstore index: " + elapsedTimeWithoutIndex + "ms.");

                    // Add a columnstore index.
                    Console.Write("Adding a columnstore to table 'Table_with_5M_rows'. This takes ~5 minute, please wait ... ");
                    sql = "CREATE CLUSTERED COLUMNSTORE INDEX columnstoreindex ON Table_with_5M_rows;";
                    using (SqlCommand command = new SqlCommand(sql, connection))
                    {
                        command.CommandTimeout = 300;
                        command.ExecuteNonQuery();
                        Console.WriteLine("Done.");
                    }

                    // Execute the same SQL query again after columnstore index was added.
                    double elapsedTimeWithIndex = SumPrice(connection);
                    Console.WriteLine("Query time WITH colmnstore index: " + elapsedTimeWithIndex + "ms.");

                    // Calculate performance gain from adding columnstore index.
                    Console.WriteLine("Performance improvement with columnstore index: "
                        + Math.Round(elapsedTimeWithoutIndex / elapsedTimeWithIndex) + "x!");

                    // Drop database.
                    connection.Close();
                    Console.Write("Dropping database 'SampleDB', press any key to continue ...");
                    Console.ReadKey(true);
                    connection.Open();
                    sql = "DROP DATABASE IF EXISTS [SampleDB];";
                    using (SqlCommand command = new SqlCommand(sql, connection))
                    {
                        command.ExecuteNonQuery();
                        Console.WriteLine("Done.");
                    }
                }
            }
            catch (SqlException e)
            {
                Console.WriteLine(e.ToString());
            }

            Console.WriteLine("All done. Press any key to finish ...");
            Console.ReadKey(true);
        }

        private static double SumPrice(SqlConnection connection)
        {
            string sql = "SELECT SUM(Price) FROM Table_with_5M_rows";
            long startTicks = DateTime.Now.Ticks;
            using (SqlCommand command = new SqlCommand(sql, connection))
            {
                try
                {
                    var sum = command.ExecuteScalar();
                    TimeSpan elapsed = TimeSpan.FromTicks(DateTime.Now.Ticks) - TimeSpan.FromTicks(startTicks);
                    return elapsed.TotalMilliseconds;
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.ToString());
                }
            }
            return 0;
        }
    }
}
