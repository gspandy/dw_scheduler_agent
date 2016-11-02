package com.ajk.dw.scheduler.quarz;

public class TriggerState {
	
	 final static int STATE_NONE = -1;//未知
	
	final static int STATE_NORMAL = 0;//正常
	
	final static int STATE_PAUSED = 1;//暂停
	
	final static int STATE_COMPLETE = 2;//完成
	
    final static int STATE_ERROR = 3;//执行错误
   
    final static int STATE_BLOCKED = 4;//阻塞
    
	public static String getState(int value) {
		
		switch (value) {
		
			case STATE_NORMAL:
				return "正常";
				
			case STATE_PAUSED:
				return "暂停";
				
			case STATE_NONE:
				return "不存在";
				
			case STATE_COMPLETE:
				return "完成";
				
			case STATE_BLOCKED:
				return "阻塞";
				
			case STATE_ERROR:
				return "错误";
				
			default:
				return "未知";
				
		} 
	}
}
