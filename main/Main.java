
import java.io.*;
import java.util.*;

public class Main{

    public static String CSV_URL_PROP = "input_trace_admission_control_url";

    private static final int INTERVAL_SIZE = 5;
    private int intervalIndex = 0;
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
            int TIME_TO_STORAGE = 1000;

            List<TaskInfo> tasksToBd = new ArrayList<>();
            List<TaskInfo> tasksOfInterval = new ArrayList<>();

            TaskInfo taskInfo;




            try {

                br = new BufferedReader(new FileReader(csvFile));
                line = br.readLine();

                long start = System.currentTimeMillis();
                System.out.println(start);

                while ((line = br.readLine()) != null) {

                    // use comma as separator
                    String[] task = line.split(cvsSplitBy);

                    double jid = Double.parseDouble(task[0]);
                    int tid = Integer.parseInt(task[1]);
                    long now = System.currentTimeMillis();
                    taskInfo = inputProcessor.getTask(jid, tid);

                    System.out.println(System.currentTimeMillis() - now);
                    System.out.println(start - System.currentTimeMillis());
                   
                    tasksToBd.add(taskInfo);

                    if (tasksToBd.size() == TIME_TO_STORAGE){
                        System.out.println("Atingiu " + TIME_TO_STORAGE);
                        outputProcessor.addTasks(tasksToBd);
                        tasksToBd.clear();
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



