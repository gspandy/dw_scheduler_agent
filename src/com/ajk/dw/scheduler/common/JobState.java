package com.ajk.dw.scheduler.common;

public enum JobState {
	
	JOB_WAITING_SIGNAL(SchedulerConstants.JOB_WAITING_SIGNAL), 
	EXECUTING(SchedulerConstants.JOB_EXECUTING), 
	KILLED(SchedulerConstants.JOB_KILLED), 
	FAIL(SchedulerConstants.JOB_FAIL), 
	SUCCESS(SchedulerConstants.JOB_SUCCESS),
	DUPSUB(SchedulerConstants.JOB_DUPSUB);

    private final int code;

    private JobState(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static JobState fromInt(int intValue) {
        JobState state;
        switch (intValue) {
        case SchedulerConstants.JOB_WAITING_SIGNAL:
            state = JOB_WAITING_SIGNAL;
            break;
        case SchedulerConstants.JOB_EXECUTING:
            state = EXECUTING;
            break;
        case SchedulerConstants.JOB_KILLED:
            state = KILLED;
            break;
        case SchedulerConstants.JOB_FAIL:
            state = FAIL;
            break;
        case SchedulerConstants.JOB_SUCCESS:
            state = SUCCESS;
            break;
        case SchedulerConstants.JOB_DUPSUB:
            state = DUPSUB;
            break;
        default:
            throw new RuntimeException(
                    "Invalid integer value for conversion to JobEvent");
        }
        return state;
    }
}
