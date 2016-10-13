
import java.io.*;
import java.util.*;

public class Main {

    public static String CSV_URL_PROP = "input_trace_admission_control_url";
    public static final String DATABASE_URL_PROP = "output_trace_database_url";


    private static final int INTERVAL_SIZE = 5;
    private static int intervalIndex = 0;
    private static InputProcessor inputProcessor;
    private static OutputProcessor outputProcessor;
    private static Map<String, Integer> mapOfTasks;

    public static void main(String[] args) throws IOException {

        try {
            Properties properties = new Properties();
            FileInputStream input = new FileInputStream(args[0]);
            properties.load(input);

            inputProcessor = new InputProcessor(properties);
            outputProcessor = new OutputProcessor(properties, DATABASE_URL_PROP);
            mapOfTasks = new HashMap<>();

            readCSV(properties);
            System.out.println("Finalizando processamento do mapa. Seu tamanho é: " + mapOfTasks.size());

            List<TaskInfo> tasksToBd = new ArrayList<>();
            List<TaskInfo> tasksOfInterval;

            do {

                tasksOfInterval = inputProcessor.getTaskInterval(intervalIndex++, getTimeInMicro(INTERVAL_SIZE));

                if (tasksOfInterval != null)
                    System.out.println("Interval index " + (intervalIndex - 1) + " = " + tasksOfInterval.size());

                filterAdmittedTasks(tasksToBd, tasksOfInterval);

                System.out.println("Adicionando " + tasksToBd.size() + " ao BD");

                outputProcessor.addTasks(tasksToBd);
                tasksToBd.clear();

            } while (tasksOfInterval != null);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static void readCSV(Properties properties){

        try {
            String csvFile = properties.getProperty(CSV_URL_PROP);
            String line = "";
            String cvsSplitBy = ",";
            BufferedReader br = new BufferedReader(new FileReader(csvFile));

            line = br.readLine();

            long start = System.currentTimeMillis();
            System.out.println(start);
            while ((line = br.readLine()) != null) {

                // use comma as separator
                String[] task = line.split(cvsSplitBy);

                double jid = Double.parseDouble(task[0]);
                int tid = Integer.parseInt(task[1]);


                String concat = "";
                concat += String.valueOf(jid) + String.valueOf(tid);
                mapOfTasks.put(concat, null);
            }
            br.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    private static void filterAdmittedTasks(List<TaskInfo> tasksToBd, List<TaskInfo> tasksOfInterval) throws IOException {

        for (TaskInfo taskInfo : tasksOfInterval) {
            String concat = "";
            concat += String.valueOf(taskInfo.getJob_id()) + String.valueOf(taskInfo.getTask_id());
            if (mapOfTasks.containsKey(concat)){
                tasksToBd.add(taskInfo);
            }
        }
    }

    public static double getTimeInMicro(double timeInMinute) {
        return timeInMinute * 60 * 1000000;
    }

}



