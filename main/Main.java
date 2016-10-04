import java.io.*;
import java.sql.ResultSet;
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

            try {

                br = new BufferedReader(new FileReader(csvFile));
                line = br.readLine();

                while ((line = br.readLine()) != null) {

                    // use comma as separator
                    String[] task = line.split(cvsSplitBy);

                    double jid = Double.parseDouble(task[8]);
                    int tid = Integer.parseInt(task[9]);

                    System.out.println("----------------------------");
                    System.out.println("jid = " + jid + "tid = " + tid);

                    ResultSet result = inputProcessor.getTask(jid, tid);
                    System.out.println("jid = " + result.getDouble("jid") + "tid = " + result.getInt("tid"));
                    System.out.println("----------------------------");

                    outputProcessor.addTask(result);

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


