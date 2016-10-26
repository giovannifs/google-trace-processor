package main;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

/**
 * Created by jvmafra on 13/10/16.
 */
public class MainCalculateAvailability {
    public static String CSV_URL_PROP = "input_trace_availability_url";


    public static void main(String[] args) throws IOException {

        int totalPriority0 = 0;
        int totalPriority1 = 0;
        int totalPriority2 = 0;
        double availPriority0 = 0;
        double availPriority1 = 0;
        double availPriority2 = 0;
        double meanPriority0 = 0;
        double meanPriority1 = 0;
        double meanPriority2 = 0;
        double meanTotal = 0;

        try {
            Properties properties = new Properties();
            FileInputStream input = new FileInputStream(args[0]);
            properties.load(input);

            try {
                String csvFile = properties.getProperty(CSV_URL_PROP);
                String line = "";
                String cvsSplitBy = ",";
                BufferedReader br = new BufferedReader(new FileReader(csvFile));

                line = br.readLine();

                while ((line = br.readLine()) != null) {

                    // use comma as separator
                    String[] task = line.split(cvsSplitBy);

                    double avail = Double.parseDouble(task[13]);
                    String priority = task[6];

                    if (priority.equalsIgnoreCase("prod")){
                        availPriority0 += avail;
                        totalPriority0++;
                    } else if (priority.equalsIgnoreCase("batch")){
                        availPriority1 += avail;
                        totalPriority1++;
                    } else {
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

        meanPriority0 = availPriority0 / totalPriority0;
        meanPriority1 = availPriority1 / totalPriority1;
        meanPriority2 = availPriority2 / totalPriority2;
        meanTotal = (availPriority0 + availPriority1 + availPriority2) / (totalPriority0 + totalPriority1 + totalPriority2);

        System.out.println("----------------------------------------");
        System.out.println("Mean Availability Priority 0 = " + meanPriority0);
        System.out.println("Mean Availability Priority 1 = " + meanPriority1);
        System.out.println("Mean Availability Priority 2 = " + meanPriority2);
        System.out.println("Mean Availability Total = " + meanTotal);
        System.out.println("----------------------------------------");


    }
}
