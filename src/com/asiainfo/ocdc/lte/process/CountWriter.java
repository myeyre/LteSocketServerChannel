package com.asiainfo.ocdc.lte.process;

//import java.io.File;
//import java.io.FileWriter;
//import java.text.SimpleDateFormat;
//import java.util.Date;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Properties;
//
//import org.apache.log4j.Logger;

public class CountWriter {
//	private static Logger logger = Logger.getLogger(CountWriter.class);
//	public static Properties prop=null;
//	private static FileWriter fw ;
//	private static HashMap<String,CountEntity> counter =new HashMap<String,CountEntity>() ;
//	private static FileWriter pw ;
//	private static FileWriter ew ;
//	private static Map<String,Long> last_sendtopictotal=new HashMap<String,Long>();
//	private static long last_receiveXdrNum=0L;
//	private static long last_receiveTotalNum=0L;
//	private static long last_sendNum=0L;
//	private static String logPath=prop.getProperty("socket.lte.log.logPath");
//	private static void init(){
//		try{
//			logger.info(""+logPath+"/receive_send.log 创建！");
//			logger.info(""+logPath+"/receive_msg_count.log 创建！");
//			logger.info(""+logPath+"/receive_badmsg.log 创建！");
//			
//			 fw = new FileWriter(new File(logPath,"receive_send.log"));
//			 pw = new FileWriter(new File(logPath,"receive_msg_count.log"));
//			 ew =new FileWriter(new File(logPath,"receive_badmsg.log"));
//		}catch(Exception e){
//			logger.error("CountWriter.init 失败！");
//			e.printStackTrace();
//		}
//	}
//	
//	public static void writepackage(String str){
//		if (pw==null)init();
//		try{
//			pw.write(str);
//			pw.flush();
//		}catch(Exception e){
//			logger.error("CountWriter.writepackage 失败！");
//			e.printStackTrace();
//		}
//	}
//	
//	public static void writeerror(String str){
//		if (ew==null)init();
//		try{
//			ew.write(str);
//			ew.flush();
//		}catch(Exception e){
//			logger.error("CountWriter.writeerror 失败！");
//			e.printStackTrace();
//		}
//	}
//	
//	public synchronized static void write(String str){
//		if (fw==null)init();
//		try{
//			
//			CountEntity entity=counter.get(str);
//			if (entity==null)return;
//			StringBuilder sb= new StringBuilder();
//			sb.append("\n");
//			sb.append("Thread-Id:"+str+"\n");
//			sb.append("receiveXdrNum:"+entity.getReceiveXdrNum()+"\n");
//			sb.append("receiveTotalNum:"+entity.getReceiveTotalNum()+"\n");
//			sb.append("sendNum:"+entity.getSendNum()+"\n");
//			Map<String,Long> sendtopic=entity.getSendTopicNum();
//			for (String topicname : sendtopic.keySet()) {
//			      long num = (long)sendtopic.get(topicname);
//			      sb.append(topicname+":"+num+"\n");
//			}
//			fw.write(sb.toString());
//			fw.flush();
//		}catch(Exception e){
//			logger.error("CountWriter.write 失败！");
//			e.printStackTrace();
//		}
//	}
//	
//	public synchronized static void writeCount(){
//		if (fw==null)init();
//		long receiveXdrNum=0L;
//		long receiveTotalNum=0L;
//		long sendNum=0L;
//		Map<String,Long> sendtopictotal=new HashMap<String,Long>();
//
//		
//		long current_receiveXdrNum=0L;
//		long current_receiveTotalNum=0L;
//		long current_sendNum=0L;
//		Map<String,Long> current_sendtopictotal=new HashMap<String,Long>();
//		try{
//			
//			if (counter.size()==0)return;
//			for (String thread:counter.keySet()){
//				CountEntity entity=counter.get(thread);
//				receiveXdrNum+=entity.getReceiveXdrNum();
//				receiveTotalNum+=entity.getReceiveTotalNum();
//				sendNum+=entity.getSendNum();
//				Map<String,Long> sendtopic=entity.getSendTopicNum();
//				if (sendtopic==null)continue;
//				for (String topicname : sendtopic.keySet()) {
//				      long num =sendtopic.get(topicname)==null?0:sendtopic.get(topicname);
//				      long numtotal=sendtopictotal.get(topicname)==null?0:sendtopictotal.get(topicname);
//				      sendtopictotal.put(topicname, numtotal+num);
//				}
//			}
//			for(String topicname : sendtopictotal.keySet()){
//				long num =sendtopictotal.get(topicname)==null?0:sendtopictotal.get(topicname);
//			      long last_num=last_sendtopictotal.get(topicname)==null?0:last_sendtopictotal.get(topicname);
//			      current_sendtopictotal.put(topicname, num-last_num);
//			}
//			last_sendtopictotal.clear();
//			last_sendtopictotal.putAll(sendtopictotal);//�����µ�ֵ�ŵ�last�У��Ա��´μ��㵱��ֵ
//			
//			current_receiveXdrNum=receiveXdrNum-last_receiveXdrNum;
//			last_receiveXdrNum=receiveXdrNum;
//			current_receiveTotalNum=receiveTotalNum-last_receiveTotalNum;
//			last_receiveTotalNum=receiveTotalNum;
//			current_sendNum=sendNum-last_sendNum;
//			last_sendNum=sendNum;
//			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//			
//			StringBuilder sb= new StringBuilder();
//			sb.append("\n");
//			sb.append(format.format(new Date())+" totalCount:\n");
//			sb.append("receiveXdrNum:"+current_receiveXdrNum+"\n");
//			sb.append("receiveTotalNum:"+current_receiveTotalNum+"\n");
//			sb.append("sendNum:"+current_sendNum+"\n");
//			for (String topicname : current_sendtopictotal.keySet()) {
//			      long num = (long)current_sendtopictotal.get(topicname);
//			      sb.append(topicname+":"+num+"\n");
//			}
//			fw.write(sb.toString());
//			fw.flush();
//		}catch(Exception e){
//			
//			logger.error("CountWriter.writeCount 失败！");
//			e.printStackTrace();
//		}
//	}
//	
//	public synchronized static void addCount(String threadId,CountEntity entity){
//		counter.put(threadId, entity);
//	}
//	public static void close(){
//		try{
//			fw.close();
//			fw=null;
//			pw.close();
//			pw=null;
//			ew.close();
//			ew=null;
//		}catch(Exception e){
//			logger.error("fw，pw，ew close 失败！");
//			e.printStackTrace();
//		}
//	}
//	public static Properties getProp() {
//		return prop;
//	}
//
//	public static void setProp(Properties prop) {
//		CountWriter.prop = prop;
//	}
}
