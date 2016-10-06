import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class InputProcessor extends DataStore{

    public static String DATABASE_URL_PROP = "input_trace_database_url";
    public static String MIN_INTERESTED_TIME_PROP = "minimum_interested_time";
    public static String MAX_INTERESTED_TIME_PROP = "maximum_interested_time";

    private double minInterestedTime;
    private double maxInterestedTime;


    public InputProcessor(Properties properties)
            throws ClassNotFoundException, SQLException {
        super(properties.getProperty(DATABASE_URL_PROP));

        minInterestedTime = isPropertySet(properties, MIN_INTERESTED_TIME_PROP) ? Double
                .parseDouble(properties.getProperty(MIN_INTERESTED_TIME_PROP)) : 0;

        maxInterestedTime = isPropertySet(properties, MAX_INTERESTED_TIME_PROP)? Double
                .parseDouble(properties.getProperty(MAX_INTERESTED_TIME_PROP)) : getMaxTraceTime() + 1;

    }

    public TaskInfo getTask(ResultSet results) {


        try {

            TaskInfo taskInfo = new TaskInfo(results.getDouble("submitTime"), results.getDouble("jid"), results.getInt("tid"),
                    results.getString("user"), results.getInt("schedulingClass"), results.getInt("priority"),
                    results.getDouble("runtime"), results.getDouble("endTime"), results.getDouble("cpuReq"),
                    results.getDouble("memReq"), results.getString("userClass"));

            return taskInfo;

        }catch (Exception e) {
            System.out.println(e.getStackTrace());
            return null;
        }
    }

    public List<TaskInfo> getTaskInterval(int intervalIndex, double intervalSize) {

        if (intervalIndex < 0){
            throw new IllegalArgumentException("Interval index must be not negative");
        }

        if (intervalSize <= 0){
            throw new IllegalArgumentException("Interval size must be positive");
        }

        if (!hasMoreEvents(intervalIndex, intervalSize)) {
            return null;
        }

        double minTime = Math.max(getMinInterestedTime(), (intervalIndex * intervalSize));
        double maxTime = Math.min(getMaxInterestedTime(), ((intervalIndex + 1) * intervalSize));


        List<TaskInfo> googleTasks = new ArrayList<>();

        Statement statement;
        Connection connection;
        try {
            connection = getConnection();

            if (connection != null) {
                statement = connection.createStatement();

                String sql = "SELECT * FROM tasks WHERE cpuReq > '0.0' AND memReq > '0.0' AND runtime > '0.0' AND submitTime >= '"
                        + minTime + "' AND submitTime < '" + maxTime + "'";

                ResultSet results = statement.executeQuery(sql);

                while (results.next()) {
                    TaskInfo task = getTask(results);
                    googleTasks.add(task);
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return googleTasks;
    }

    public boolean hasMoreEvents(int intervalIndex,
                                 double intervalSize) {
        return (intervalIndex >= 0 && (intervalIndex * intervalSize) <= getMaxInterestedTime());
    }

    private boolean isPropertySet(Properties properties, String propKey) {
        return properties.getProperty(propKey) != null;
    }

    protected double getMaxTraceTime() throws ClassNotFoundException,
            SQLException {
        Statement statement = null;
        Connection connection = null;

        try {
            Class.forName(DATASTORE_SQLITE_DRIVER);
            connection = getConnection();

            if (connection != null) {

                statement = connection.createStatement();

                // getting the max submitTime from database
                ResultSet results = statement
                        .executeQuery("SELECT MAX(submitTime) FROM tasks WHERE cpuReq > '0' AND memReq > '0'");

                while (results.next()) {
                    return results.getDouble("MAX(submitTime)");
                }
            }
        } finally {
            close(statement, connection);
        }
        // It should never return this value.
        return -1;
    }

    public double getMinInterestedTime() {
        return minInterestedTime;
    }

    public double getMaxInterestedTime() {
        return maxInterestedTime;
    }

}
