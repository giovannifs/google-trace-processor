import java.sql.*;
import java.util.List;
import java.util.Properties;

public class OutputProcessor extends DataStore{


    private static final String TABLE_NAME = "tasks";

    /**
     * Constructor that create a table where the tasks will be inserted
     * @param properties - File of configuration
     * @param destiny - Name of parameter existed in configuration that contains the URL
     */
    public OutputProcessor(Properties properties, String destiny) {
        super(properties.getProperty(destiny));

        Statement statement = null;
        Connection connection = null;
        try {

            Class.forName(DATASTORE_SQLITE_DRIVER);

            connection = getConnection();
            statement = connection.createStatement();
            statement
                    .execute("CREATE TABLE IF NOT EXISTS tasks("
                            + "submitTime REAL, "
                            + "jid REAL, "
                            + "tid INTEGER, "
                            + "user TEXT, "
                            + "schedulingClass INTEGER, "
                            + "priority INTEGER, "
                            + "runtime REAL, "
                            + "endTime REAL, "
                            + "cpuReq REAL, "
                            + "memReq REAL, "
                            + "userClass TEXT"
                            + ")");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close(statement, connection);
        }
    }

    private static final String INSERT_TASK_SQL = "INSERT INTO " + TABLE_NAME
            + " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";


    /**
     * Operation that add a list of tasks into a BD
     * @param list_of_tasks that contains objects of TaskInfo
     * @return A boolean that represents the success of operation
     */
    public boolean addTasks(List<TaskInfo> list_of_tasks) {
        if (list_of_tasks == null) {
            return false;
        }

        PreparedStatement insertMemberStatement = null;

        Connection connection = null;

        try {
            connection = getConnection();
            connection.setAutoCommit(false);

            insertMemberStatement = connection.prepareStatement(INSERT_TASK_SQL);
            insertMemberStatement = connection
                    .prepareStatement(INSERT_TASK_SQL);

            for (TaskInfo taskInfo : list_of_tasks) {
                addTask(insertMemberStatement, taskInfo);
            }

            int[] executeBatch = insertMemberStatement.executeBatch();

            if (executionFailed(connection, executeBatch)){
                connection.rollback();
                return false;
            }

            connection.commit();
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                if (connection != null) {
                    connection.rollback();
                }
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            return false;
        } finally {
            close(insertMemberStatement, connection);
        }
    }

    private boolean executionFailed(Connection connection, int[] executeBatch)
            throws SQLException {
        for (int i : executeBatch) {
            if (i == PreparedStatement.EXECUTE_FAILED) {
                return true;
            }
        }
        return false;
    }


    /**
     * Operation that add a task info into a batch to prepare that will be inserted into BD
     * @param insertMemberStatement
     * @param taskInfo - A unique object of TaskInfo
     * @throws SQLException
     */
    private void addTask(PreparedStatement insertMemberStatement,
                         TaskInfo taskInfo) throws SQLException {
        insertMemberStatement.setDouble(1, taskInfo.getSubmitTime());
        insertMemberStatement.setDouble(2, taskInfo.getJob_id());
        insertMemberStatement.setInt(3, taskInfo.getTask_id());
        insertMemberStatement.setString(4, taskInfo.getUser());
        insertMemberStatement.setInt(5, taskInfo.getSchedulingClass());
        insertMemberStatement.setInt(6, taskInfo.getPriority());
        insertMemberStatement.setDouble(7, taskInfo.getRuntime());
        insertMemberStatement.setDouble(8, taskInfo.getEndTime());
        insertMemberStatement.setDouble(9, taskInfo.getCpuReq());
        insertMemberStatement.setDouble(10, taskInfo.getMemReq());
        insertMemberStatement.setString(11, taskInfo.getUserClass());
        insertMemberStatement.addBatch();
    }

}