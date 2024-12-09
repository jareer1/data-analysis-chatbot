class backendServices:
    def getSqlData(self, db_type, username, password, host, port, database_name, **kwargs):
        """
        Generates a connection string for structured databases (PostgreSQL, MySQL, etc.).

        :param db_type: Type of database (e.g., "postgresql", "mysql").
        :param username: Username for the database.
        :param password: Password for the database.
        :param host: Database server's host address.
        :param port: Port number of the database server.
        :param database_name: Name of the database.
        :param kwargs: Additional optional parameters (e.g., sslmode, charset).
        :return: A formatted connection string.
        """
        # Ensure required fields are provided
        if not all([db_type, username, password, host, port, database_name]):
            raise ValueError("Missing required parameters to build the connection string.")

        # Construct the base connection string
        connection_string = f"{db_type}://{username}:{password}@{host}:{port}/{database_name}"

        # Append optional parameters, if provided
        if kwargs:
            optional_params = "&".join([f"{key}={value}" for key, value in kwargs.items()])
            connection_string += f"?{optional_params}"

        return connection_string
