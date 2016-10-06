
import java.io.*;
import java.util.*;

public class Main {

    public static String CSV_URL_PROP = "input_trace_admission_control_url";

    private static final int INTERVAL_SIZE = 5;
    private static int intervalIndex = 0;
    private static InputProcessor inputProcessor;
    private static OutputProcessor outputProcessor;

    public static void main(String[] args) {

        try {
            Properties properties = new Properties();
            FileInputStream input = new FileInputStream(args[0]);
            properties.load(input);

            inputProcessor = new InputProcessor(properties);
            outputProcessor = new OutputProcessor(properties);


            String csvFile = properties.getProperty(CSV_URL_PROP);
            BufferedReader br = new BufferedReader(new FileReader(csvFile));

            List<TaskInfo> tasksToBd = new ArrayList<>();

            long searchTime = System.currentTimeMillis();
            List<TaskInfo> tasksOfInterval = inputProcessor.getTaskInterval(intervalIndex, INTERVAL_SIZE);
            System.out.println("Search duration in bd is:" + (System.currentTimeMillis() - searchTime) + " milliseconds");


            while (tasksOfInterval != null) {

                long now = System.currentTimeMillis();
                filterAdmittedTasks(br, tasksToBd, tasksOfInterval);
                System.out.println("Filter admitted tasks in:" + (System.currentTimeMillis() - now) + " milliseconds");

                outputProcessor.addTasks(tasksToBd);
                System.out.println("Add tasks of interval: " + intervalIndex);
                tasksToBd.clear();

                tasksOfInterval = inputProcessor.getTaskInterval(intervalIndex++, INTERVAL_SIZE);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static void filterAdmittedTasks(BufferedReader br, List<TaskInfo> tasksToBd, List<TaskInfo> tasksOfInterval) {

        String line = "";
        String cvsSplitBy = ",";

        for (TaskInfo taskInfo : tasksOfInterval) {

            try {

                line = br.readLine();
                long start = System.currentTimeMillis();
                System.out.println(start);

                while ((line = br.readLine()) != null) {

                    // use comma as separator
                    String[] task = line.split(cvsSplitBy);

                    double jid = Double.parseDouble(task[0]);
                    int tid = Integer.parseInt(task[1]);

                    if (taskInfo.getJob_id() == jid && taskInfo.getTask_id() == tid) {
                        tasksToBd.add(taskInfo);
                        break;
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
        }
    }
}



