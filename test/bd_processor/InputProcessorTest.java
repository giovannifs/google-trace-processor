package bd_processor;

import junit.framework.*;
import org.junit.*;
import org.junit.Assert;
import org.junit.Assert.*;
import org.junit.Test;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by Alessandro Lia Fook Santos and Joao Victor Mafra on 26/10/16.
 */
public class InputProcessorTest {

    private final double ACCEPTABLE_DIFFERENCE = 0.00001;
    private final static int NUMBER_OF_TASKS = 100;

    private static String databaseFile = "inputTraceTest.sqlite3";
    private static String databaseURL = "jdbc:sqlite:" + databaseFile;

    private static String databaseFile2 = "inputTraceTest_2.sqlite3";
    private static String databaseURL2 = "jdbc:sqlite:" + databaseFile2;

    private static double DEFAULT_RUNTIME = 1000;
    private static Properties properties;
    private static Properties properties2;

    @BeforeClass
    public static void setUp() throws ClassNotFoundException, SQLException {
        createAndPopulateTestDatabase();
    }

    @Before
    public void init() {
        properties = new Properties();
        properties.setProperty(InputProcessor.DATABASE_URL_PROP, databaseURL);

        properties2 = new Properties();
        properties2.setProperty(InputProcessor.DATABASE_URL_PROP, databaseURL2);
    }

    private static void createAndPopulateTestDatabase() throws ClassNotFoundException, SQLException {
        Class.forName(DataStore.DATASTORE_SQLITE_DRIVER);
        Connection connection = DriverManager.getConnection(databaseURL);

        if (connection != null) {
            // Creating the database
            Statement statement = connection.createStatement();
            statement.execute("CREATE TABLE IF NOT EXISTS tasks("
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
                    + "userClass TEXT" + ")");
            statement.close();

            String INSERT_CLOUDLET_SQL = "INSERT INTO tasks"
                    + " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            // populating the database
            for (int i = 1; i <= NUMBER_OF_TASKS; i++) {
                PreparedStatement insertMemberStatement = connection
                        .prepareStatement(INSERT_CLOUDLET_SQL);
                insertMemberStatement.setDouble(1, getTimeInMicro(i)); // submit time
                insertMemberStatement.setDouble(2, -1); // jid is not important for now
                insertMemberStatement.setInt(3, -1); // tid is not important for now
                insertMemberStatement.setString(4, "user"); // user is not important for now
                insertMemberStatement.setInt(5, -1); // scheduling class is not important for now
                insertMemberStatement.setInt(6, -1); // priority is not important for now
                insertMemberStatement.setDouble(7, DEFAULT_RUNTIME); // runtime
                insertMemberStatement.setDouble(8, i + DEFAULT_RUNTIME); // endtime
                insertMemberStatement.setDouble(9, 1); // cpuReq is not important for now
                insertMemberStatement.setDouble(10, 1); // memReq is not important for now
                insertMemberStatement.setString(11, "userClass"); // userClass is not important for now
                insertMemberStatement.execute();
            }
            connection.close();
        }
    }

    @AfterClass
    public static void tearDown() {
        new File(databaseFile).delete();
        new File(databaseFile2).delete();
    }



    @Test
    public void testGetMaxSubmitTime() throws Exception {
        InputProcessor inputTrace = new InputProcessor(properties); // initialize a trace
        Assert.assertEquals(0, inputTrace.getMinInterestedTime(), ACCEPTABLE_DIFFERENCE); // test default minInterestTime
        Assert.assertEquals(getTimeInMicro(NUMBER_OF_TASKS), inputTrace.getMaxTraceTime(), ACCEPTABLE_DIFFERENCE); // test default maxTraceTime
        Assert.assertEquals(getTimeInMicro(NUMBER_OF_TASKS) + 1, inputTrace.getMaxInterestedTime(), ACCEPTABLE_DIFFERENCE);// test default maxInterestTime
        // maxTraceTime = 6.0E9

    }

    @Test
    public void testSetValuesofInterestTime() throws Exception{
        double newMax = getTimeInMicro(60);
        double newMin = getTimeInMicro(20);
        properties.setProperty(InputProcessor.MAX_INTERESTED_TIME_PROP, String.valueOf(newMax));
        properties.setProperty(InputProcessor.MIN_INTERESTED_TIME_PROP, String.valueOf(newMin));
        InputProcessor inputTrace = new InputProcessor(properties);
        Assert.assertEquals(getTimeInMicro(60), inputTrace.getMaxInterestedTime(), ACCEPTABLE_DIFFERENCE);
        Assert.assertEquals(getTimeInMicro(20), inputTrace.getMinInterestedTime(), ACCEPTABLE_DIFFERENCE);
    }

    @Test
    public void testGetGTask1MicroInterval() throws Exception {
        InputProcessor inputTrace = new InputProcessor(
                properties);


        for (int i = 1; i <= NUMBER_OF_TASKS; i++) {
            Assert.assertEquals(1,
                    inputTrace.getTaskInterval(i, getTimeInMicro(1)).size()); // test if tasks are divided in groups of one
        }

        // test if interval index equals to 0 is empty
        Assert.assertEquals(0,
                inputTrace.getTaskInterval(0, getTimeInMicro(1)).size());

        // test limits of intervals, using IntervalSize equals to 2
        Assert.assertEquals(1, inputTrace.getTaskInterval(0, getTimeInMicro(2)).size());
        Assert.assertEquals(1, inputTrace.getTaskInterval(NUMBER_OF_TASKS/2, getTimeInMicro(2)).size());


        // test all sizes of intervals in pairs without limits
        for (int i = 1; i < NUMBER_OF_TASKS/2; i++) {
            Assert.assertEquals(2,
                    inputTrace.getTaskInterval(i, getTimeInMicro(2)).size()); // test if tasks are divided in groups of one
        }
    }

    @Test
    public void testGetGTaskBiggerMicroInterval() throws Exception {
        InputProcessor inputTrace = new InputProcessor(
                properties);

        // because the submitTime started from 1 instead of 0
        Assert.assertEquals(NUMBER_OF_TASKS/2 - 1,
                inputTrace.getTaskInterval(0, getTimeInMicro(NUMBER_OF_TASKS/2)).size());

        Assert.assertEquals(NUMBER_OF_TASKS/2,
                inputTrace.getTaskInterval(1, getTimeInMicro(NUMBER_OF_TASKS/2)).size());

        // because the minTime of this interval is exactly the maximum time of the fake trace
        Assert.assertEquals(1,
                inputTrace.getTaskInterval(2, getTimeInMicro(NUMBER_OF_TASKS/2)).size());

        // because the minTime of this interval is bigger than maximum time of the fake trace
        Assert.assertNull(inputTrace.getTaskInterval(3, getTimeInMicro(NUMBER_OF_TASKS/2)));
    }

    @Test
    public void testGetGTaskInvalidInterval() throws Exception {
        InputProcessor inputTrace = new InputProcessor(
                properties);
        Assert.assertNull(inputTrace.getTaskInterval(NUMBER_OF_TASKS + 1,
                getTimeInMicro(1)));
    }

    @Test
            (expected = IllegalArgumentException.class)
    public void testGetGTaskInvalidInterval2() throws Exception {
        InputProcessor inputTrace = new InputProcessor(
                properties);
        inputTrace.getTaskInterval(-1,
                getTimeInMicro(1));
    }

    @Test
    public void testGetGTaskIntervalWithMinValue() throws Exception {
        double min = getTimeInMicro(51);
        properties.setProperty(InputProcessor.MIN_INTERESTED_TIME_PROP, String.valueOf(min));
        InputProcessor inputTrace = new InputProcessor(
                properties);

        // test the number of tasks between time 51 and 100 (although the Interval size is greater than maxSubmitTime)
        Assert.assertEquals(NUMBER_OF_TASKS/2,
                inputTrace.getTaskInterval(0, getTimeInMicro(NUMBER_OF_TASKS + 1)).size());

    }

    @Test
    public void testGetGTaskIntervalWithMaxValue() throws Exception {
        double max = getTimeInMicro(NUMBER_OF_TASKS/2) + 1;
        properties.setProperty(InputProcessor.MAX_INTERESTED_TIME_PROP, String.valueOf(max));
        InputProcessor inputTrace = new InputProcessor(
                properties);

        // test the number of tasks between time 0 and 50 (although the Interval size is greater than maxSubmitTime)
        Assert.assertEquals(NUMBER_OF_TASKS/2,
                inputTrace.getTaskInterval(0, getTimeInMicro(NUMBER_OF_TASKS + 1)).size());

    }

    @Test
            (expected = IllegalArgumentException.class)
    public void testConstructorWithMinValueNegative() throws Exception{
        double min = getTimeInMicro(-1);
        properties.setProperty(InputProcessor.MIN_INTERESTED_TIME_PROP, String.valueOf(min));
        InputProcessor inputTrace = new InputProcessor(
                properties);
    }

    @Test
            (expected = IllegalArgumentException.class)
    public void testConstructorWithMaxValueNegative() throws Exception{
        double max = getTimeInMicro(-1);
        properties.setProperty(InputProcessor.MAX_INTERESTED_TIME_PROP, String.valueOf(max));
        InputProcessor inputTrace = new InputProcessor(
                properties);
    }

    @Test
    public void testConstructorWithMaxValueEqualsNull2() throws Exception{
        InputProcessor inputTrace = new InputProcessor(
                properties);
        Assert.assertEquals(inputTrace.getMaxInterestedTime(), inputTrace.getMaxTraceTime() + 1, ACCEPTABLE_DIFFERENCE);
        Assert.assertEquals(inputTrace.getMinInterestedTime(), 0, ACCEPTABLE_DIFFERENCE);
    }

    @Test
    public void testConstructorWithMinValueEqualsNull2() throws Exception{
        InputProcessor inputTrace = new InputProcessor(
                properties);
        Assert.assertEquals(inputTrace.getMaxInterestedTime(), inputTrace.getMaxTraceTime() + 1, ACCEPTABLE_DIFFERENCE);
        Assert.assertEquals(inputTrace.getMinInterestedTime(), 0, ACCEPTABLE_DIFFERENCE);
    }

    @Test
            (expected = IllegalArgumentException.class)
    public void testGetGTaskIntervalWithNegativeIntervalIndex() throws Exception{
        InputProcessor inputTrace = new InputProcessor(
                properties);

        inputTrace.getTaskInterval(-1, 5);
    }

    @Test
            (expected = IllegalArgumentException.class)
    public void testGetGTaskIntervalWithNegativeIntervalSize() throws Exception{
        InputProcessor inputTrace = new InputProcessor(
                properties);

        inputTrace.getTaskInterval(0, -1);
    }

    @Test
            (expected = IllegalArgumentException.class)
    public void testGetGTaskIntervalWithIntervalSizeEquals0() throws Exception{
        InputProcessor inputTrace = new InputProcessor(
                properties);

        inputTrace.getTaskInterval(0, 0);
    }



    @Test
    public void testGetGTaskIntervalWithMaxValueGreaterThanMaxSubmitTime() throws Exception {
        double max = getTimeInMicro(NUMBER_OF_TASKS + 1);
        properties.setProperty(InputProcessor.MAX_INTERESTED_TIME_PROP, String.valueOf(max));
        InputProcessor inputTrace = new InputProcessor(
                properties);

        // test the number of tasks between time 0 and 100 (Because 101 is not considered)
        Assert.assertEquals(NUMBER_OF_TASKS,
                inputTrace.getTaskInterval(0, getTimeInMicro(NUMBER_OF_TASKS + 1)).size());

        // remember to test with max time greater than max submit time

    }

    @Test
    public void testGetGTaskIntervalWithMinMaxValues() throws Exception {
        double min = getTimeInMicro(20);
        double max = getTimeInMicro(80);
        properties.setProperty(InputProcessor.MIN_INTERESTED_TIME_PROP, String.valueOf(min));
        properties.setProperty(InputProcessor.MAX_INTERESTED_TIME_PROP, String.valueOf(max));
        InputProcessor inputTrace = new InputProcessor(
                properties);

        Assert.assertEquals(80 - 20,
                inputTrace.getTaskInterval(0, getTimeInMicro(NUMBER_OF_TASKS + 1)).size());

        // remember to test limit values (eg. 100 and 101, 0 and 0, 100 and 100)

        min = getTimeInMicro(NUMBER_OF_TASKS);
        max = getTimeInMicro(NUMBER_OF_TASKS + 1); // adding 1 minute
        properties.setProperty(InputProcessor.MIN_INTERESTED_TIME_PROP, String.valueOf(min));
        properties.setProperty(InputProcessor.MAX_INTERESTED_TIME_PROP, String.valueOf(max));
        inputTrace = new InputProcessor(
                properties);

        Assert.assertEquals(1,
                inputTrace.getTaskInterval(0, getTimeInMicro(NUMBER_OF_TASKS + 1)).size());

        min = getTimeInMicro(0);
        max = getTimeInMicro(0);
        properties.setProperty(InputProcessor.MIN_INTERESTED_TIME_PROP, String.valueOf(min));
        properties.setProperty(InputProcessor.MAX_INTERESTED_TIME_PROP, String.valueOf(max));
        inputTrace = new InputProcessor(
                properties);

        Assert.assertEquals(0,
                inputTrace.getTaskInterval(0, getTimeInMicro(NUMBER_OF_TASKS + 1)).size());


        min = getTimeInMicro(NUMBER_OF_TASKS);
        max = getTimeInMicro(NUMBER_OF_TASKS) + 1; // adding 1 microsecond
        properties.setProperty(InputProcessor.MIN_INTERESTED_TIME_PROP, String.valueOf(min));
        properties.setProperty(InputProcessor.MAX_INTERESTED_TIME_PROP, String.valueOf(max));
        inputTrace = new InputProcessor(
                properties);

        Assert.assertEquals(1,
                inputTrace.getTaskInterval(0, getTimeInMicro(NUMBER_OF_TASKS + 1)).size());

    }

    @Test
    public void testHasMoreEvents() throws Exception{
        InputProcessor inputTrace = new InputProcessor(
                properties);

        for (int i = -2; i <= NUMBER_OF_TASKS + 1; i++) {

            if (i >= 101 || i < 0){
                Assert.assertFalse(inputTrace.hasMoreEvents(i, getTimeInMicro(1)));

            } else {
                Assert.assertTrue(inputTrace.hasMoreEvents(i, getTimeInMicro(1)));
            }
        }

    }

    @Test
    public void testLimitsGetTaskIntervalEqualsList() throws Exception {

        //tests if the return of getGoogleTaskInterval is equals to list expected

        InputProcessor inputTrace = new InputProcessor(
                properties);

        //tests first interval with one element on the list
        Assert.assertEquals(generateListOfGoogleTasks(1, 1), inputTrace.getTaskInterval(1, getTimeInMicro(1)));

        // tests empty list in interval 0
        Assert.assertEquals(generateListOfGoogleTasks(1, 0), inputTrace.getTaskInterval(0, getTimeInMicro(1)));

    }

    @Test
    public void testGetTaskIntervalEqualsList() throws Exception {

        InputProcessor inputTrace = new InputProcessor(
                properties);

        Assert.assertEquals(generateListOfGoogleTasks(1, (NUMBER_OF_TASKS / 2) - 1),
                inputTrace.getTaskInterval(0, getTimeInMicro(NUMBER_OF_TASKS / 2)));

        Assert.assertEquals(generateListOfGoogleTasks(NUMBER_OF_TASKS / 2, NUMBER_OF_TASKS - 1),
                inputTrace.getTaskInterval(1, getTimeInMicro(NUMBER_OF_TASKS / 2)));

        // because the minTime of this interval is exactly the maximum time of the fake trace
        Assert.assertEquals(generateListOfGoogleTasks(NUMBER_OF_TASKS, NUMBER_OF_TASKS),
                inputTrace.getTaskInterval(2, getTimeInMicro(NUMBER_OF_TASKS / 2)));

    }

    @Test
    public void testGetGTaskMicroInterval2() throws Exception {

        // inserting a second google task in time 1

        Class.forName(DataStore.DATASTORE_SQLITE_DRIVER);
        Connection connection2 = DriverManager.getConnection(databaseURL2);

        if (connection2 != null) {
            // Creating the database
            Statement statement = connection2.createStatement();
            statement.execute("CREATE TABLE IF NOT EXISTS tasks("
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
                    + "userClass TEXT" + ")");
            statement.close();

            String INSERT_CLOUDLET_SQL = "INSERT INTO tasks"
                    + " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            // populating the database
            for (int i = 0; i <= 1; i++) {
                PreparedStatement insertMemberStatement = connection2
                        .prepareStatement(INSERT_CLOUDLET_SQL);
                insertMemberStatement.setDouble(1, getTimeInMicro(1)); // submit time
                insertMemberStatement.setDouble(2, -1); // jid is not important for now
                insertMemberStatement.setInt(3, -1); // tid is not important for now
                insertMemberStatement.setNString(4, "user"); // user is not important for now
                insertMemberStatement.setInt(5, -1); // scheduling class is not important for now
                insertMemberStatement.setInt(6, i); // priority equals 7 and 8 will be converted to priority 0 and 1, respectively
                insertMemberStatement.setDouble(7, DEFAULT_RUNTIME); // runtime
                insertMemberStatement.setDouble(8, i + DEFAULT_RUNTIME); // endtime
                insertMemberStatement.setDouble(9, 1); // cpuReq is not important for now
                insertMemberStatement.setDouble(10, 1); // memReq is not important for now
                insertMemberStatement.setNString(11, "userClass"); // userClass is not important for now
                insertMemberStatement.execute();
            }
            connection2.close();
        }

        InputProcessor inputTrace = new InputProcessor(
                properties2);

        // tests if we have 0 task at time 0
        Assert.assertEquals(0,
                inputTrace.getTaskInterval(0, getTimeInMicro(1)).size());

        // tests if we have 2 tasks at time 1

        Assert.assertEquals(2,
                inputTrace.getTaskInterval(1, getTimeInMicro(1)).size());

        // tests if we have 2 tasks in total
        Assert.assertEquals(2,
                inputTrace.getTaskInterval(0, getTimeInMicro(NUMBER_OF_TASKS + 1)).size());

        // testing priority
        List<TaskInfo> lista = inputTrace.getTaskInterval(0, getTimeInMicro(NUMBER_OF_TASKS + 1));
        Assert.assertEquals(0, lista.get(0).getPriority());
        Assert.assertEquals(1, lista.get(1).getPriority());

    }

    private static double getTimeInMicro(double timeInMinutes) {
        return timeInMinutes * 60 * 1000000;
    }

    private static List<TaskInfo> generateListOfGoogleTasks (int startIndex, int endIndex) {

        int scheduler = -1; // scheduling class is not important for now
        double cpuReq =  1; // cpuReq is not important for now
        double memReq = 1; // memReq is not important for now
        int tid = -1;
        int jid = -1;
        String userClass = "userClass";
        String user = "user";
        int priority = -1;

        List<TaskInfo> listOfTasks = new ArrayList<TaskInfo>();

        for (int i = startIndex; i <= endIndex; i++) {

            double submitTime =  getTimeInMicro(i); // submit time

            TaskInfo task = new TaskInfo(submitTime, jid, tid, user, scheduler, priority, DEFAULT_RUNTIME, i + DEFAULT_RUNTIME, cpuReq, memReq, userClass);
            listOfTasks.add(task);
        }

        return listOfTasks;
    }
}