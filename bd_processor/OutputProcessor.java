import java.sql.*;
import java.util.Properties;

public class OutputProcessor extends DataStore{

    public static final String DATABASE_URL_PROP = "output_tasks_database_url";

    private static final String TABLE_NAME = "tasks";

    public OutputProcessor(Properties properties) {
        super(properties.getProperty(DATABASE_URL_PROP));

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

    public boolean addTask(ResultSet results) {
        if (results == null) {
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

            addTask(insertMemberStatement, results);
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

    private void addTask(PreparedStatement insertMemberStatement,
                         ResultSet results) throws SQLException {
        insertMemberStatement.setDouble(1, results.getDouble("submitTime"));
        insertMemberStatement.setDouble(2, results.getDouble("jid"));
        insertMemberStatement.setInt(3, results.getInt("tid"));
        insertMemberStatement.setString(4, results.getString("user"));
        insertMemberStatement.setInt(5, results.getInt("schedulingClass"));
        insertMemberStatement.setInt(6, results.getInt("priority"));
        insertMemberStatement.setDouble(7, results.getDouble("runtime"));
        insertMemberStatement.setDouble(8, results.getDouble("endTime"));
        insertMemberStatement.setDouble(9, results.getDouble("cpuReq"));
        insertMemberStatement.setDouble(10, results.getDouble("memReq"));
        insertMemberStatement.setString(11, results.getString("userClass"));
        insertMemberStatement.addBatch();
    }

}