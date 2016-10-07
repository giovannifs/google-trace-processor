
import java.io.*;
import java.util.*;

public class Main {

    public static String CSV_URL_PROP = "input_trace_admission_control_url";

    private static final int INTERVAL_SIZE = 5;
    private static int intervalIndex = 0;
    private static InputProcessor inputProcessor;
    private static OutputProcessor outputProcessor;

    public static void main(String[] args) throws IOException {

        try {
            Properties properties = new Properties();
            FileInputStream input = new FileInputStream(args[0]);
            properties.load(input);

            inputProcessor = new InputProcessor(properties);
            outputProcessor = new OutputProcessor(properties);


            String csvFile = properties.getProperty(CSV_URL_PROP);
            RandomAccessFile raf = new RandomAccessFile(new File(csvFile), "r");

            List<TaskInfo> tasksToBd = new ArrayList<>();
            List<TaskInfo> tasksOfInterval = inputProcessor.getTaskInterval(intervalIndex, getTimeInMicro(INTERVAL_SIZE));

            while (tasksOfInterval != null) {

                filterAdmittedTasks(raf, tasksToBd, tasksOfInterval);

                outputProcessor.addTasks(tasksToBd);
                tasksToBd.clear();

                tasksOfInterval = inputProcessor.getTaskInterval(intervalIndex++, getTimeInMicro(INTERVAL_SIZE));
            }
            raf.close();


        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    private static void filterAdmittedTasks(RandomAccessFile raf, List<TaskInfo> tasksToBd, List<TaskInfo> tasksOfInterval) throws IOException {

        String line = "";
        String cvsSplitBy = ",";

        for (TaskInfo taskInfo : tasksOfInterval) {

            try {

                raf.seek(0);
                line = raf.readLine();

                long start = System.currentTimeMillis();
                System.out.println(start);

                while ((line = raf.readLine()) != null) {

                    // use comma as separator
                    String[] task = line.split(cvsSplitBy);

                    double jid = Double.parseDouble(task[0]);
                    int tid = Integer.parseInt(task[1]);

                    if (taskInfo.getJob_id() == jid && taskInfo.getTask_id() == tid) {
                        System.out.println("A task com jid=" + taskInfo.getJob_id() + " e com id=" + taskInfo.getTask_id() + " foi adicionada a lista.");
                        tasksToBd.add(taskInfo);
                        break;
                    }
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static double getTimeInMicro(double timeInMinute) {
        return timeInMinute * 60 * 1000000;
    }

}



