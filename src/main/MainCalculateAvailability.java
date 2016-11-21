package main;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

/**
 * The main that calculates the mean of availability by priority from a csv file
 * Created by Joao Victor Mafra and Alessandro Lia Fook Santos on 13/10/16.
 */
public class MainCalculateAvailability {

    public static String CSV_URL_PROP = "input_trace_availability_url";

    public static final int POSITION_OF_AVAILABILITY = 13;
    public static final int POSITION_OF_PRIORITY_NAME = 6;
    public static final String CSV_SPLIT_BY = ",";

    public static final String PROD = "prod";
    public static final String BATCH = "batch";
    private static final String FREE = "free";

    public static void main(String[] args) throws IOException {

        int totalPriority0 = 0;
        int totalPriority1 = 0;
        int totalPriority2 = 0;
        double availPriority0 = 0;
        double availPriority1 = 0;
        double availPriority2 = 0;

        try {

            Properties properties = new Properties();
            String fileName = args[0];
            FileInputStream input = new FileInputStream(fileName);
            properties.load(input);

            try {

                String csvFile = properties.getProperty(CSV_URL_PROP);
                BufferedReader br = new BufferedReader(new FileReader(csvFile));

                String line = br.readLine();

                while ((line = br.readLine()) != null) {

                    // use comma as separator
                    String[] task = line.split(CSV_SPLIT_BY);

                    double avail = Double.parseDouble(task[POSITION_OF_AVAILABILITY]);
                    String priority = task[POSITION_OF_PRIORITY_NAME];

                    if (priority.equalsIgnoreCase(PROD)){
                        availPriority0 += avail;
                        totalPriority0++;

                    } else if (priority.equalsIgnoreCase(BATCH)){
                        availPriority1 += avail;
                        totalPriority1++;

                    } else if (priority.equalsIgnoreCase(FREE)) {
                        availPriority2 += avail;
                        totalPriority2++;
                    }
                }

                br.close();

            } catch (FileNotFoundException e) {
                e.printStackTrace();

            } catch (IOException e) {
                e.printStackTrace();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        double meanPriority0 = availPriority0 / totalPriority0;
        double meanPriority1 = availPriority1 / totalPriority1;
        double meanPriority2 = availPriority2 / totalPriority2;
        double meanTotal = (availPriority0 + availPriority1 + availPriority2)
                            / (totalPriority0 + totalPriority1 + totalPriority2);

        System.out.println("----------------------------------------");
        System.out.println("Mean Availability Priority 0 = " + meanPriority0);
        System.out.println("Mean Availability Priority 1 = " + meanPriority1);
        System.out.println("Mean Availability Priority 2 = " + meanPriority2);
        System.out.println("Mean Availability Total = " + meanTotal);
        System.out.println("----------------------------------------");
    }
}
