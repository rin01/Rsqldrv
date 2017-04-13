using System;
using System.Collections.Generic;

namespace Rsqldrv.SqlClient
{
    // class containing the connection string information.
    class ConnStringAttributes
    {
        internal string serverAddr = "";
        internal int serverPort = SqlConnection.DEFAULT_SERVER_PORT;
        internal string login = "";
        internal string password = "";
        internal string database = "";
        internal int connectionTimeout = SqlConnection.CONNECTION_TIMEOUT;

        // splits the connection string attribute=value pairs into ConnStringAttributes fields.
        internal ConnStringAttributes(string connString)
        {
            string[] items = connString.Split(';');

            foreach (string item in items)
            {
                if (item.Trim() == "") // consecutive or terminating semicolons, e.g.   "server = 127.0.0.1; ; login=john;"
                    continue;

                string[] parts = item.Split('=');
                if (parts.Length != 2)
                    throw new DriverException("Connection string must contain attr=val pairs separated by semicolon.");

                string attr = parts[0].Trim().ToLower();
                if (attr == "")
                    throw new DriverException("Connection string: attributes cannot be empty string.");

                string val = parts[1].Trim();
                if (val == "")
                    throw new DriverException(String.Format("Connection string: value for attribute \"{0}\" cannot be empty string.", attr));

                switch (attr)
                {
                    case "server":
                    case "data source":
                    case "address":
                    case "addr":
                        string serverAddr = val;
                        int serverPort = SqlConnection.DEFAULT_SERVER_PORT;

                        if (val.IndexOf(':') >= 0)
                        {
                            string[] addrPort = val.Split(':');
                            if (addrPort.Length != 2)
                                throw new DriverException(String.Format("Connection string: invalid server and port \"{0}\" value.", val));

                            serverAddr = addrPort[0];

                            if (Int32.TryParse(addrPort[1], out serverPort) == false)
                                throw new DriverException(String.Format("Connection string: invalid port \"{0}\" value.", addrPort[1]));
                        }

                        if (serverAddr.Trim() == "(local)")
                            serverAddr = "127.0.0.1";

                        this.serverAddr = serverAddr;
                        this.serverPort = serverPort;
                        break;

                    case "login":
                    case "user id":
                        this.login = val.ToLower();
                        break;

                    case "password":
                    case "pwd":
                        this.password = val; // original case
                        break;

                    case "database":
                    case "initial catalog":
                        this.database = val.ToLower();
                        break;

                    case "connection timeout":
                    case "connect timeout":
                    case "timeout":
                        int connectionTimeout = 0;
                        if (Int32.TryParse(val, out connectionTimeout) == false)
                                throw new DriverException(String.Format("Connection string: invalid connection timeout \"{0}\" value.", val));

                        this.connectionTimeout = connectionTimeout;
                        break;

                    default:
                        throw new DriverException(String.Format("Connection string attribute \"{0}\" is not supported.", attr));
                }
            }
        }

    }
}
