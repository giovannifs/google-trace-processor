package bd_processor;

/**
 * Created by jvmafra on 05/10/16.
 */
public class TaskInfo {
    private double job_id, submitTime, runtime, endTime, cpuReq, memReq;
    private int task_id, priority, schedulingClass;
    private String user, userClass;

    /**
     * Constructor that contains information about Vm
     * @param submitTime - submitTime of VM
     * @param job_id - jid of VM in database
     * @param task_id - tid of VM in database
     * @param user - user of VM
     * @param schedulingClass - schedulingClass of VM
     * @param priority - priority of VM (0, 1 or 2)
     * @param runtime - runtime of VM
     * @param endTime - submitTime of VM
     * @param cpuReq - cpu requested by VM
     * @param memReq - memory requested by VM
     * @param userClass - user class of VM
     */
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


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TaskInfo taskInfo = (TaskInfo) o;

        if (Double.compare(taskInfo.job_id, job_id) != 0) return false;
        if (Double.compare(taskInfo.submitTime, submitTime) != 0) return false;
        if (Double.compare(taskInfo.runtime, runtime) != 0) return false;
        if (Double.compare(taskInfo.endTime, endTime) != 0) return false;
        if (Double.compare(taskInfo.cpuReq, cpuReq) != 0) return false;
        if (Double.compare(taskInfo.memReq, memReq) != 0) return false;
        if (task_id != taskInfo.task_id) return false;
        if (priority != taskInfo.priority) return false;
        if (schedulingClass != taskInfo.schedulingClass) return false;
        if (!user.equals(taskInfo.user)) return false;
        return userClass.equals(taskInfo.userClass);

    }
}
