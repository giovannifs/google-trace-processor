package bd_processor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by jvmafra on 04/10/16.
 */
public class DataStore {

    protected static final String DATASTORE_SQLITE_DRIVER = "org.sqlite.JDBC";

    private String databaseURL;

    public DataStore(String databaseURL) {

        setDatabaseURL(databaseURL);
    }

    protected Connection getConnection() throws SQLException {

        try {
            return DriverManager.getConnection(getDatabaseURL());

        } catch (SQLException e) {
            throw e;
        }
    }

    protected void close(Statement statement, Connection conn) {

        if (statement != null) {

            try {

                if (!statement.isClosed()) {
                    statement.close();
                }

            } catch (SQLException e) {
            }
        }

        if (conn != null) {

            try {

                if (!conn.isClosed()) {
                    conn.close();
                }

            } catch (SQLException e) {
            }
        }
    }

    protected String getDatabaseURL() {
        return databaseURL;
    }

    protected void setDatabaseURL(String databaseURL) {
        this.databaseURL = databaseURL;
    }
}