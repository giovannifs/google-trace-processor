package main;

import bd_processor.OutputProcessor;
import bd_processor.TaskInfo;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * The main that generates a BD with expected features.
 * Created by Alessandro Lia Fook Santos and Joao Victor Mafra on 13/10/16.
 */
public class MainGenerateBd {

    private static final int UNUSED_INT = 1;
    private static final double UNUSED_DOUBLE = 1.0;
    private static final String UNUSED_NAME = "empty";
    public static int taskId = 0;
    public static double jobId = 0.0;

    private static final int NUMBER_OF_TASKS = 6603;

    private static final String DATABASE_URL_PROP = "output_trace_availability_url";

    public static void main(String[] args) {

        List<TaskInfo> taskList = new ArrayList<>();
        createTasks(taskList);

        try {

            Properties properties = new Properties();
            FileInputStream input = new FileInputStream(args[0]);
            properties.load(input);

            OutputProcessor outputProcessor = new OutputProcessor(properties, DATABASE_URL_PROP);
            outputProcessor.addTasks(taskList);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * Operation that creates and generates a set of tasks with selected features by adding the list recieved as a
     * parameter.
     * @param taskList - The list that will contain all tasks genereted.
     */
    private static void createTasks(List<TaskInfo> taskList) {


        double submitTimeInMinutes = 0d;
        int valueOfPriority = 11;
        double finishTimeInMinutes = 8;
        double cpuRequested = 0.5;
        String userClass = "prod";

        createAndAddTasksToList(taskList, submitTimeInMinutes, valueOfPriority, finishTimeInMinutes, cpuRequested,
                                userClass);

        System.out.println(taskList.size());
        System.out.println(taskId);

        valueOfPriority = 7;
        finishTimeInMinutes = 5;
        cpuRequested = 0.3;
        userClass = "batch";

        createAndAddTasksToList(taskList, submitTimeInMinutes, valueOfPriority, finishTimeInMinutes, cpuRequested,
                userClass);

        System.out.println(taskList.size());
        System.out.println(taskId);

        valueOfPriority = 0;
        finishTimeInMinutes = 2;
        cpuRequested = 0.2;
        userClass = "free";

        createAndAddTasksToList(taskList, submitTimeInMinutes, valueOfPriority, finishTimeInMinutes, cpuRequested,
                userClass);

        System.out.println(taskList.size());
        System.out.println(taskId);

        submitTimeInMinutes = 1;
        valueOfPriority = 11;
        finishTimeInMinutes = 2;
        cpuRequested = 0.6;
        userClass = "prod";

        createAndAddTasksToList(taskList, submitTimeInMinutes, valueOfPriority, finishTimeInMinutes, cpuRequested,
                userClass);

        System.out.println(taskList.size());
        System.out.println(taskId);
    }

    private static void createAndAddTasksToList(List<TaskInfo> taskList, double subtmitTimeInMinutes, int valueOfPriority, double finishTimeInMinutes, double cpuRequested, String userClass) {

        for (int i = 0; i < NUMBER_OF_TASKS; i++) {

            TaskInfo task = new TaskInfo(getTimeInMicro(subtmitTimeInMinutes), jobId++, taskId++, UNUSED_NAME,
                    UNUSED_INT, valueOfPriority, getTimeInMicro(finishTimeInMinutes),
                    getTimeInMicro(finishTimeInMinutes), cpuRequested, UNUSED_DOUBLE, userClass);
            taskList.add(task);
        }
    }

    /**
     * Operation that recieves the time in minutes and corvert to time in microsseconds.
     * @param timeInMinute
     * @return The value recieved as parametter in microsseconds.
     */
    public static double getTimeInMicro(double timeInMinute) {
        return timeInMinute * 60 * 1000000;
    }
}
