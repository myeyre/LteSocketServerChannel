package com.asiainfo.ocsp.lte.signal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

import com.asiainfo.ocdc.lte.process.LteCacheServer;
import org.apache.log4j.Logger;
import com.asiainfo.ocdc.streaming.producer.SendUtil;

/**
 * @since 2016.05.26
 * @author 宿荣全
 * @comment 机群版socket服务
 */
public class LTEDespatcherServer {

	private static Logger logger = Logger.getLogger(LTEDespatcherServer.class);
	public static LinkedBlockingQueue<byte[]> lbkAllMsg = new LinkedBlockingQueue<byte[]>();
	public static void main(String[] args) {

		// 加载配置文件
		SendUtil sendUtil = new SendUtil();
		Properties prop = sendUtil.prop;

		ExecutorService executorPool = null;

		// timer 定时打印各个队列大小
		boolean timer_flg = Boolean.valueOf(prop.getProperty("socket.lte.queue.size.timer.flg").trim());
		// timer 定时监控周期
		long interval = Long.parseLong(prop.getProperty("socket.lte.queue.size.time.interval").trim());
		if (timer_flg) {
			java.util.Timer timer = new java.util.Timer();
			timer.schedule(new DespatcherTimerTaskTest(), 1000, interval);
		}

		// 解析socket work node ip and port
		String wokerNodes = prop.getProperty("socket.lte.signal.cluster.node").trim();
		String[] wokerNodeList = wokerNodes.split(",");

		if (wokerNodeList.length < 0) {
			System.out.println("请列明work node IP和端口【socket.lte.signal.cluster.node】");
			logger.error("请列明work node IP和端口【socket.lte.signal.cluster.node】");
			return;
		}

		// 装载socket work node ip and port
		ArrayList<String[][]> wokerList = new ArrayList<String[][]>(wokerNodeList.length);
		for (String woker : wokerNodeList) {
			String[] ipPort = woker.split(":");
			String[][] workinfo = new String[][] { { ipPort[0], ipPort[1] } };
			wokerList.add(workinfo);
		}
		int workdNodeSize = wokerList.size();
		logger.info("woker node 节点数:" + workdNodeSize);
		try {
			// 获取线程池
			executorPool = sendUtil.getExecutorPool();
			logger.info("LTEDespatcherServer 线程池启动成功！");

//			// 多线程接收socket 数据
//			int Recever_thread_num = Integer.parseInt(prop.getProperty("socket.lte.recever.thread.num").trim());
//			logger.info("Processdata 线程数：" + Recever_thread_num);
//			for (int i = 0; i < Recever_thread_num; i++) {
//				executorPool.execute(new ReceveTask(lbkSocket, lbkAllMsg));
//			}

			// 多线程分发socket数据，为数据均匀，最好是woker
			// node的整数倍［socket.lte.signal.despatch.thread.num］
			int sort_type_num = Integer.parseInt(prop.getProperty("socket.lte.signal.despatch.thread.num").trim());
			int workerIndex = 0;
			for (int i = 0; i < sort_type_num; i++) {
				if (i % workdNodeSize == 0) {
					workerIndex = 0;
				}
				String[][] workinfo = wokerList.get(workerIndex++);
				executorPool.execute(new DespatcherSendTask(lbkAllMsg, workinfo));
			}

			// 启动serverSocket
			int serverPort = Integer.parseInt(prop.getProperty("socket.lte.socketServer.port").trim());
			LteCacheServer server = new LteCacheServer(serverPort);
        	server.listenAction(lbkAllMsg);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}