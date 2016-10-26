package main;

import bd_processor.OutputProcessor;
import bd_processor.TaskInfo;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by Alessandro Lia Fook Santos and Joao Victor Mafra on 13/10/16.
 */
public class MainGenerateBd {

    private static final int UNUSED_INT = 1;
    private static final double UNUSED_DOUBLE = 1.0;
    private static final String UNUSED_NAME = "empty";

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

    private static void createTasks(List<TaskInfo> taskList) {

        int taskId = 0;
        double jobId = 0.0;

        for (int i = 0; i < NUMBER_OF_TASKS; i++) {

            TaskInfo task = new TaskInfo(getTimeInMicro(0), jobId++, taskId++, UNUSED_NAME, UNUSED_INT, 11,
                                getTimeInMicro(8), getTimeInMicro(8), 0.5, UNUSED_DOUBLE, "prod");
            taskList.add(task);
        }

        System.out.println(taskList.size());
        System.out.println(taskId);

        for (int i = 0; i < NUMBER_OF_TASKS; i++) {

            TaskInfo task = new TaskInfo(getTimeInMicro(0), jobId++, taskId++, UNUSED_NAME, UNUSED_INT, 7,
                    getTimeInMicro(5), getTimeInMicro(5), 0.3, UNUSED_DOUBLE, "batch");

            taskList.add(task);
        }

        System.out.println(taskList.size());
        System.out.println(taskId);

        for (int i = 0; i < NUMBER_OF_TASKS; i++) {

            TaskInfo task = new TaskInfo(getTimeInMicro(0), jobId++, taskId++, UNUSED_NAME, UNUSED_INT, 0,
                    getTimeInMicro(2), getTimeInMicro(2), 0.2, UNUSED_DOUBLE, "free");

            taskList.add(task);
        }

        System.out.println(taskList.size());
        System.out.println(taskId);

        for (int i = 0; i < NUMBER_OF_TASKS; i++) {

            TaskInfo task = new TaskInfo(getTimeInMicro(1), jobId++, taskId++, UNUSED_NAME, UNUSED_INT, 11,
                    getTimeInMicro(2), getTimeInMicro(2), 0.6, UNUSED_DOUBLE, "prod");

            taskList.add(task);
        }

        System.out.println(taskList.size());
        System.out.println(taskId);
    }

    public static double getTimeInMicro(double timeInMinute) {
        return timeInMinute * 60 * 1000000;
    }
}
