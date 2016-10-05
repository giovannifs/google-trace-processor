import com.sun.javafx.tk.Toolkit;

import java.io.*;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Main{

    public static String CSV_URL_PROP = "input_trace_admission_control_url";

    private static InputProcessor inputProcessor;
    private static OutputProcessor outputProcessor;

    public static void main(String[] args){

        try{
            Properties properties = new Properties();
            FileInputStream input = new FileInputStream(args[0]);
            properties.load(input);

            inputProcessor = new InputProcessor(properties);
            outputProcessor = new OutputProcessor(properties);


            String csvFile = properties.getProperty(CSV_URL_PROP);
            BufferedReader br = null;
            String line = "";
            String cvsSplitBy = ",";
            int TIME_TO_STORAGE = 5000;
            List<TaskInfo> list_of_tasks = new ArrayList<>();
            TaskInfo taskInfo;

            try {

                br = new BufferedReader(new FileReader(csvFile));
                line = br.readLine();

                while ((line = br.readLine()) != null) {

                    // use comma as separator
                    String[] task = line.split(cvsSplitBy);

                    double jid = Double.parseDouble(task[0]);
                    int tid = Integer.parseInt(task[1]);

                    taskInfo = inputProcessor.getTask(jid, tid);
                    list_of_tasks.add(taskInfo);

                    if (list_of_tasks.size() == TIME_TO_STORAGE){
                        System.out.println("Atingiu " + TIME_TO_STORAGE);
                        outputProcessor.addTasks(list_of_tasks);
                        list_of_tasks.clear();
                    }

                }

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (br != null) {
                    try {
                        br.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }


        } catch (Exception e) {
            e.printStackTrace();
        }

    }




}


