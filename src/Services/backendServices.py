class BackendServices:
    def getSqlData(self, db_type, username, password, host, port, database_name, **kwargs):
        if not all([db_type, username, password, host, port, database_name]):
            raise ValueError("Missing required parameters to build the connection string.")

        connection_string = f"{db_type}://{username}:{password}@{host}:{port}/{database_name}"
        #
        # if kwargs:
        #     optional_params = "&".join([f"{key}={value}" for key, value in kwargs.items()])
        #     connection_string += f"?{optional_params}"

        return connection_string
