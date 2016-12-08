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

import com.asiainfo.ocdc.lte.process.LTETypeShort;
import com.asiainfo.ocdc.lte.process.LteSaveFile;
import com.asiainfo.ocdc.lte.process.LteSendTask;
import com.asiainfo.ocdc.lte.process.Processdata;

public class LteSocketServer {

	private static Logger logger = Logger.getLogger(SocketAutoProducer.class);
	private static LinkedBlockingQueue<Socket> lbkSocket = new LinkedBlockingQueue<Socket>();
	private static LinkedBlockingQueue<byte[]> lbkAllMsg = new LinkedBlockingQueue<byte[]>();
	private static LinkedBlockingQueue<ArrayList<KeyedMessage<String, String>>> msg_queue = new LinkedBlockingQueue<ArrayList<KeyedMessage<String, String>>>();
	
	public static void main(String[] args) {
		
		// 加载配置文件
		SendUtil sendUtil = new SendUtil();
		Properties prop = sendUtil.prop;
		
		ServerSocket serverSocket = null;
		ExecutorService executorPool = null;
		try {
			logger.info("LteSocketServer 启动...");
			// 启动serverSocket
			int serverPort = Integer.parseInt(prop.getProperty("socket.lte.socketServer.port").trim());
			serverSocket = new ServerSocket(serverPort);
			
			// 获取线程池
			executorPool = sendUtil.getExecutorPool();
			logger.info("LteSocketServer 线程池启动成功！");
			// 多线程解析socket 数据
			int process_thread_num = Integer.parseInt(prop.getProperty("socket.lte.process.thread.num").trim());
			for (int i = 0; i< process_thread_num;i++){
				executorPool.execute(new Processdata(lbkSocket,lbkAllMsg));
			}
			
			// 多线程分类处理数据
			int sort_type_num = Integer.parseInt(prop.getProperty("socket.lte.process.sort.type.num").trim());
			for (int i = 0; i< sort_type_num;i++){
				executorPool.execute(new LTETypeShort(lbkAllMsg,msg_queue,prop));
			}
			
			// 多线程向kafka发送数据
			int partitions_num = Integer.parseInt(prop.getProperty("socket.lte.partitions.num").trim());
			for (int i = 0; i< partitions_num;i++){
				executorPool.execute(new LteSendTask(msg_queue,prop));
			}
			
			while (true){
				Socket socket = serverSocket.accept();
				lbkSocket.offer(socket);
				logger.info("LteSocketServer lbkSocket.offer(socket)");
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