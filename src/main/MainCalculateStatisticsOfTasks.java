package main;

import bd_processor.InputProcessor;
import bd_processor.TaskInfo;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by jvmafra on 21/02/17.
 */
public class MainCalculateStatisticsOfTasks {

    public static String TXT_URL_PROP = "input_trace_admission_control_url";
    public static final int POSITION_OF_USER_CLASS = 4;
    public static final int POSITION_OF_AVAILABILITY = 8;
    public static final String SPLIT_TXT_BY = " ";


    public static void main(String[] args) throws IOException {

        try {
            Properties properties = new Properties();
            String fileName = args[0];
            FileInputStream input = new FileInputStream(fileName);
            properties.load(input);


            calculateStatistics(properties);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static void calculateStatistics(Properties properties) {

        int totalOfProdTasks = 0;
        int totalOfBatchTasks = 0;
        int totalOfFreeTasks = 0;

        double totalOfProdTasksFulfilled = 0;
        double totalOfBatchTasksFulfilled = 0;
        double totalOfFreeTasksFulfilled = 0;

        double sumOfProdAvailability = 0;
        double sumOfBatchAvailability = 0;
        double sumOfFreeAvailability = 0;


        try {

            String csvFile = properties.getProperty(TXT_URL_PROP);
            BufferedReader br = new BufferedReader(new FileReader(csvFile));
            String line = br.readLine();

            while ((line = br.readLine()) != null) {

                // use comma as separator
                String[] task = line.split(SPLIT_TXT_BY);

                String userClass = task[POSITION_OF_USER_CLASS];
                double availability = Double.parseDouble(task[POSITION_OF_AVAILABILITY]);

                if (userClass.equals("prod")){
                    totalOfProdTasks += 1;
                    sumOfProdAvailability += availability;
                    if (availability >= 1){
                        totalOfProdTasksFulfilled += 1;
                    }
                } else if (userClass.equals("batch")){
                    totalOfBatchTasks += 1;
                    sumOfBatchAvailability += availability;
                    if (availability >= 0.9){
                        totalOfBatchTasksFulfilled += 1;
                    }
                } else {
                    totalOfFreeTasks += 1;
                    sumOfFreeAvailability += availability;
                    if (availability >= 0.5){
                        totalOfFreeTasksFulfilled += 1;
                    }
                }


            }

            br.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();

        } catch (IOException e) {
            e.printStackTrace();
        }


        System.out.println("TOTAL OF TASKS: " + (totalOfProdTasks + totalOfBatchTasks + totalOfFreeTasks));
        System.out.println("TOTAL OF PROD TASKS: " + totalOfProdTasks);
        System.out.println("TOTAL OF BATCH TASKS: " + totalOfBatchTasks);
        System.out.println("TOTAL OF FREE TASKS: " + totalOfFreeTasks);
        System.out.println("----------------------------------------------------------");
        System.out.println("MEAN AVAILABILITY OF PROD CLASS: " + (sumOfProdAvailability/totalOfProdTasks));
        System.out.println("MEAN AVAILABILITY OF BATCH CLASS: " + (sumOfBatchAvailability/totalOfBatchTasks));
        System.out.println("MEAN AVAILABILITY OF FREE CLASS: " + (sumOfFreeAvailability/totalOfFreeTasks));
        System.out.println("----------------------------------------------------------");
        System.out.println("SLO FULFILLMENT OF PROD CLASS: " + (totalOfProdTasksFulfilled/totalOfProdTasks));
        System.out.println("SLO FULFILLMENT OF BATCH CLASS: " + (totalOfBatchTasksFulfilled/totalOfBatchTasks));
        System.out.println("SLO FULFILLMENT OF FREE CLASS: " + (totalOfFreeTasksFulfilled/totalOfFreeTasks));


    }


}
