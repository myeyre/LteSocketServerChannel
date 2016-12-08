package com.asiainfo.ocdc.streaming.producer;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

import kafka.producer.KeyedMessage;

import org.apache.log4j.Logger;

import com.asiainfo.ocdc.lte.process.LTETypeSignalPort;
import com.asiainfo.ocdc.lte.process.LteSendTask;
import com.asiainfo.ocdc.lte.process.ReceveTask;

public class LteSocketSignalPortServer {

	private static Logger logger = Logger.getLogger(LteSocketSignalPortServer.class);
	public static LinkedBlockingQueue<Socket> lbkSocket = new LinkedBlockingQueue<Socket>();
	public static LinkedBlockingQueue<byte[]> lbkAllMsg = new LinkedBlockingQueue<byte[]>();
	public static LinkedBlockingQueue<ArrayList<KeyedMessage<String, String>>> msg_queue = new LinkedBlockingQueue<ArrayList<KeyedMessage<String, String>>>();
	
	public static void main(String[] args) {
		
		java.util.Timer timer = new java.util.Timer();
        timer.schedule(new TimerTaskTest(), 1000, 5000);
        
		// 加载配置文件
		SendUtil sendUtil = new SendUtil();
		Properties prop = sendUtil.prop;
		
		ServerSocket serverSocket = null;
		ExecutorService executorPool = null;
		try {
			// 获取线程池
			executorPool = sendUtil.getExecutorPool();
			logger.info("LteSocketServer 线程池启动成功！");
			
			// 启动serverSocket
			int serverPort = Integer.parseInt(prop.getProperty("socket.lte.socketServer.port").trim());
			serverSocket = new ServerSocket(serverPort);
			logger.info("LteSocketServer 启动成功！");
			
			// 多线程接收socket 数据
			int Recever_thread_num = Integer.parseInt(prop.getProperty("socket.lte.recever.thread.num").trim());
			logger.info("Processdata 线程数："+Recever_thread_num);
			for (int i = 0; i< Recever_thread_num;i++){
				executorPool.execute(new ReceveTask(lbkSocket,lbkAllMsg));
			}
			
			// 多线程分类处理数据
			int sort_type_num = Integer.parseInt(prop.getProperty("socket.lte.process.sort.type.num").trim());
			for (int i = 0; i< sort_type_num;i++){
				executorPool.execute(new LTETypeSignalPort(lbkAllMsg,msg_queue,prop));
			}
			
			// 多线程向kafka发送数据
			int partitions_num = Integer.parseInt(prop.getProperty("socket.lte.partitions.num").trim());
			for (int i = 0; i< partitions_num;i++){
				executorPool.execute(new LteSendTask(msg_queue,prop));
			}
			int i=1;
			while (true){
				Socket socket = serverSocket.accept();
				lbkSocket.offer(socket);
				System.out.println("socket 链路id:"+ i++);
			}
		} catch (IOException e) {
			logger.error("LteSocketServer 启动失败！");
			e.printStackTrace();
		}finally {
			try {
				serverSocket.close();
				executorPool.shutdownNow();
			} catch (IOException e) {
				logger.error("serverSocket close 失败！");
				e.printStackTrace();
			}
		}
	}
}