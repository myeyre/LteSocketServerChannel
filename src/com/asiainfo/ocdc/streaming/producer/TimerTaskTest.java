package com.asiainfo.ocdc.streaming.producer;

import java.util.TimerTask;

public class TimerTaskTest extends TimerTask{ 
     
    @Override 
    public void run() { 
    	int lbkSocketSize = LteSocketSignalPortServer.lbkSocket.size();
    	int lbkAllMsgSize = LteSocketSignalPortServer.lbkAllMsg.size();
    	int msg_queueSize = LteSocketSignalPortServer.msg_queue.size();
    	System.out.println("lbkSocket 队列长度=============================================："+lbkSocketSize);
    	System.out.println("lbkAllMsg 队列长度=============================================："+lbkAllMsgSize);
		System.out.println("msg_queue 队列长度=============================================："+msg_queueSize);
    } 
}