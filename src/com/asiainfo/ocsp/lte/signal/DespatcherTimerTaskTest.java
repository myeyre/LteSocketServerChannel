package com.asiainfo.ocsp.lte.signal;

import java.util.HashMap;
import java.util.TimerTask;

import com.asiainfo.ocdc.lte.process.LteCacheServer;
import com.asiainfo.ocdc.lte.process.SocketChannelProcess;


public class DespatcherTimerTaskTest  extends TimerTask{ 
   public HashMap<Integer, Long> old_count_map = new HashMap<Integer,Long>();
		   
		   
   @Override 
   public void run() { 
   	System.out.println("lbkAllMsg 队列长度===========："+LTEDespatcherServer.lbkAllMsg.size());
   	long total_count=0;
 	for (SocketChannelProcess scp:LteCacheServer.countList){
 		Long old_count = old_count_map.get(scp.channelParamBean.getChannelID()) == null ? 0 :old_count_map.get(scp.channelParamBean.getChannelID());
 		Long cur_count = scp.channelParamBean.getTotalCount();
 		total_count += cur_count - old_count;
 		System.out.println("socket接收数据量统计--链路ID："+scp.channelParamBean.getChannelID() + "接收条数:"+(cur_count - old_count));
 		
 		old_count_map.put(scp.channelParamBean.getChannelID(), cur_count);
 	}
   	
 	System.out.println("1分钟接收总条数: "+total_count);
   } 
}
