/**
 * Created by jvmafra on 05/10/16.
 */
public class TaskInfo {
    private double job_id, submitTime, runtime, endTime, cpuReq, memReq;
    private int task_id, priority, schedulingClass;
    private String user, userClass;

    public TaskInfo(double submitTime, double job_id, int task_id, String user, int schedulingClass, int priority, double runtime,
                    double endTime, double cpuReq, double memReq, String userClass){
        setJob_id(job_id);
        setTask_id(task_id);
        setSubmitTime(submitTime);
        setUser(user);
        setSchedulingClass(schedulingClass);
        setPriority(priority);
        setRuntime(runtime);
        setEndTime(endTime);
        setCpuReq(cpuReq);
        setMemReq(memReq);
        setUserClass(userClass);
    }

    public void setSubmitTime(double submitTime) {
        this.submitTime = submitTime;
    }

    public void setRuntime(double runtime) {
        this.runtime = runtime;
    }

    public void setEndTime(double endTime) {
        this.endTime = endTime;
    }

    public void setCpuReq(double cpuReq) {
        this.cpuReq = cpuReq;
    }

    public void setMemReq(double memReq) {
        this.memReq = memReq;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public void setSchedulingClass(int schedulingClass) {
        this.schedulingClass = schedulingClass;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setUserClass(String userClass) {
        this.userClass = userClass;
    }

    public void setJob_id(double jid){
        this.job_id = jid;
    }

    public void setTask_id(int tid){
        this.task_id = tid;
    }

    public double getSubmitTime() {

        return submitTime;
    }

    public double getRuntime() {
        return runtime;
    }

    public double getEndTime() {
        return endTime;
    }

    public double getCpuReq() {
        return cpuReq;
    }

    public double getMemReq() {
        return memReq;
    }

    public int getPriority() {
        return priority;
    }

    public int getSchedulingClass() {
        return schedulingClass;
    }

    public String getUser() {
        return user;
    }

    public String getUserClass() {
        return userClass;
    }

    public double getJob_id(){
        return job_id;
    }

    public int getTask_id(){
        return task_id;
    }
}
