package main;

import java.io.FileInputStream;
import java.util.List;
import java.util.Properties;

import bd_processor.InputProcessor;
import bd_processor.OutputProcessor;
import bd_processor.TaskInfo;

public class ConvertTimesMain {

    private static final int COLLECT_INTERVAL_SIZE = 60;
	private static InputProcessor inputProcessor;
    private static OutputProcessor outputProcessor;
    
	public static void main(String[] args) {
		try {
			Properties properties = new Properties();
			String fileName = args[0];
			FileInputStream input = new FileInputStream(fileName);
			properties.load(input);
			
			inputProcessor = new InputProcessor(properties);
            outputProcessor = new OutputProcessor(properties, "output_trace_database_url");

            int epochSize = Integer.parseInt(properties.getProperty("epoch_size"));
            
            int intervalIndex = 0;

            List<TaskInfo> tasksOfInterval = inputProcessor.getTaskInterval(intervalIndex, getTimeInMicro(COLLECT_INTERVAL_SIZE));

            while (tasksOfInterval != null) {

                if (tasksOfInterval != null)
                    System.out.println("Interval index " + intervalIndex + " = " + tasksOfInterval.size());

                List<TaskInfo> tasksWithConvertedTimes = convertTimeFromTasks(tasksOfInterval, epochSize);

                System.out.println("Adding " + tasksWithConvertedTimes.size() + " to BD");

                outputProcessor.addTasks(tasksWithConvertedTimes);
//                tasksWithConvertedTimes.clear();
                intervalIndex++;
                
                tasksOfInterval = inputProcessor.getTaskInterval(intervalIndex, getTimeInMicro(COLLECT_INTERVAL_SIZE));
            }			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static List<TaskInfo> convertTimeFromTasks(
			List<TaskInfo> tasksOfInterval, int epochSize) {
		for (TaskInfo taskInfo : tasksOfInterval) {
			taskInfo.setSubmitTime(Math.ceil(taskInfo.getSubmitTime() / epochSize));
			taskInfo.setRuntime(Math.ceil(taskInfo.getRuntime() / epochSize));
			taskInfo.setEndTime(Math.ceil(taskInfo.getEndTime() / epochSize));			
		}
		return tasksOfInterval;
	}

    public static double getTimeInMicro(double timeInMinute) {
        return timeInMinute * 60 * 1000000;
    }
}
