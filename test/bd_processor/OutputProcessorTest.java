package bd_processor;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * Created by Alessandro Lia Fook Santos and Joao Victor Mafra on 26/10/16.
 */
public class OutputProcessorTest {
    public static final double TIME = 1;

    private static String databaseFile = "TaskDataStoreTest.sqlite3";
    private static String databaseURL = "jdbc:sqlite:" + databaseFile;

    public static final String DATABASE_URL_PROP = "output_trace_database_url";

    private static Properties properties;

    OutputProcessor dataStore;

    private List<TaskInfo> taskStates;

    private TaskInfo task1, task2, task3, task4;

    private int taskId, priority, jobId, schedulingClass;
    private double cpuReq, finishingTime, runtime, submitTime;
    private String user, userClass;
    private double memReq;

    @Before
    public void setUp() {

        // creating the dataStore
        properties = new Properties();
        properties.setProperty(DATABASE_URL_PROP, databaseURL);

        // creating data store
        dataStore = new OutputProcessor(properties, DATABASE_URL_PROP);

        // creating the list of task states
        taskStates = new ArrayList<>();

        taskId = 0;
        priority = 1;
        cpuReq = 0.02;
        submitTime = 0;
        runtime = 0.001;
        finishingTime = 0.001;
        user = "user";
        userClass = "userClass";
        jobId = -1;
        schedulingClass = -1;
        memReq = 1;

        task1 = new TaskInfo(submitTime, jobId, taskId++, user, schedulingClass, priority - 1, runtime, finishingTime, cpuReq, memReq, userClass);
        task2 = new TaskInfo(submitTime, jobId, taskId++, user, schedulingClass, priority, runtime, finishingTime, cpuReq, memReq, userClass);
        task3 = new TaskInfo(submitTime, jobId, taskId++, user, schedulingClass, priority + 1, runtime, finishingTime, cpuReq, memReq, userClass);
    }

    @After
    public void tearDown() {
        new File(databaseFile).delete();
    }

    @Test
    public void testAddNullList() {
        Assert.assertFalse(dataStore.addTasks(null));
    }

    @Test
    public void testAddEmptyList() {
        Assert.assertTrue(dataStore.addTasks(taskStates));
        Assert.assertEquals(0, dataStore.getAllTasks().size());
    }

    @Test
    public void testAddList() {

        taskStates.add(task1);
        Assert.assertTrue(dataStore.addTasks(taskStates));
        Assert.assertArrayEquals(taskStates.toArray(), dataStore.getAllTasks().toArray());

    }

    @Test
    public void testAddList2() {

        taskStates.add(task1);
        taskStates.add(task2);
        Assert.assertTrue(dataStore.addTasks(taskStates));
        Assert.assertArrayEquals(taskStates.toArray(), dataStore.getAllTasks().toArray());

        taskStates.add(task3);
        Assert.assertFalse(Arrays.equals(taskStates.toArray(), dataStore.getAllTasks().toArray()));
    }

    @Test
    public void testAddList3() {

        int taskId = 0;

        for (int i = 0; i < 50; i++) {
            task1 = new TaskInfo(submitTime, jobId, taskId++, user, schedulingClass, priority - 1, runtime, finishingTime, cpuReq, memReq, userClass);
            taskStates.add(task1);
        }

        Assert.assertTrue(dataStore.addTasks(taskStates));
        Assert.assertArrayEquals(taskStates.toArray(), dataStore.getAllTasks().toArray());

        List<TaskInfo> taskStates2 = new ArrayList<>();

        for (int i = 0; i < 50; i++) {
            task1 = new TaskInfo(submitTime, jobId, taskId++, user, schedulingClass, priority - 1, runtime, finishingTime, cpuReq, memReq, userClass);
            taskStates.add(task1);
            taskStates2.add(task1);
        }

        Assert.assertTrue(dataStore.addTasks(taskStates2));
        Assert.assertArrayEquals(taskStates.toArray(), dataStore.getAllTasks().toArray());
        Assert.assertFalse(Arrays.equals(taskStates2.toArray(), dataStore.getAllTasks().toArray()));
    }

}