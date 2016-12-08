package com.asiainfo.ocsp.lte.signal;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

/**
 * @since 2016.05.26
 * @author 宿荣全
 * @comment 机群版socket服务
 */
public class DespatcherSendTask implements Runnable {
	private static Logger logger = Logger.getLogger(DespatcherSendTask.class);
	public LinkedBlockingQueue<byte[]> lbkAllMsg = null;
	private String[][] workinfo = null;

	public DespatcherSendTask(LinkedBlockingQueue<byte[]> lbkAllMsg,
			String[][] workinfo) {
		this.lbkAllMsg = lbkAllMsg;
		this.workinfo = workinfo;
	}

	public void run() {
		String ip = workinfo[0][0];
		int port = Integer.parseInt(workinfo[0][1]);
		System.out.println("DespatcherSendTask.java:---SEND机器信息ip:[" + ip + "]+port:[" + port + "]");
		logger.info("DespatcherSendTask IP:[" + ip + "] port:[" + port + "]启动成功！");
		ObjectOutputStream out = null;
		Socket client = null;
		byte[] msg = null;

		while (true) {
			try {
				logger.info("socket创建连结 IP:[" + ip + "] port:[" + port + "]");
				System.out.println("socket创建连结 IP:[" + ip + "] port:[" + port + "]");
				client = new Socket(ip, port);
				out = new ObjectOutputStream(client.getOutputStream());
				while (true) {
					msg = lbkAllMsg.take();
					out.writeObject(msg);
					out.flush();
					out.reset();
				}
			} catch (Exception e) {
				logger.error("socket连结中断！IP:[" + ip + "] port:[" + port + "]");
				System.out.println("socket连结中断！IP:[" + ip + "] port:[" + port + "]");
				try {
					// 读入端会报：java.io.EOFException BlockDataInputStream.peekByte
					if (out != null){
						out.close();
					}
					if (client != null){
						client.close();
					}
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				e.printStackTrace();
			}
		}
	}
}