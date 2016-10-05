import java.sql.*;
import java.util.Properties;

public class InputProcessor extends DataStore{
    public static String DATABASE_URL_PROP = "input_trace_database_url";

    public InputProcessor(Properties properties)
            throws ClassNotFoundException, SQLException {
        super(properties.getProperty(DATABASE_URL_PROP));

    }

    public TaskInfo getTask(double jobId, int taskId) {
        Statement statement = null;
        Connection connection;
        try {
            connection = getConnection();

            if (connection != null) {
                statement = connection.createStatement();

                String sql = "SELECT * FROM tasks WHERE jid == '" + jobId + "' AND tid == '" + taskId + "'";

                ResultSet results = statement.executeQuery(sql);

                TaskInfo taskInfo = new TaskInfo(results.getDouble("submitTime"), results.getDouble("jid"), results.getInt("tid"),
                        results.getString("user"), results.getInt("schedulingClass"), results.getInt("priority"),
                        results.getDouble("runtime"), results.getDouble("endTime"), results.getDouble("cpuReq"),
                        results.getDouble("memReq"), results.getString("userClass"));

                return taskInfo;

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}