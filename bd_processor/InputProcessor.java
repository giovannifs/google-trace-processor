import java.sql.*;
import java.util.Properties;

public class InputProcessor extends DataStore{
    public static String DATABASE_URL_PROP = "input_trace_database_url";

    public InputProcessor(Properties properties)
            throws ClassNotFoundException, SQLException {
        super(properties.getProperty(DATABASE_URL_PROP));

    }

    public ResultSet getTask(double jobId, int taskId) {
        Statement statement = null;
        Connection connection;
        try {
            connection = getConnection();

            if (connection != null) {
                statement = connection.createStatement();

                String sql = "SELECT * FROM tasks WHERE jid == '" + jobId + "' AND tid == '" + taskId + "'";

                ResultSet results = statement.executeQuery(sql);

                return results;

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}