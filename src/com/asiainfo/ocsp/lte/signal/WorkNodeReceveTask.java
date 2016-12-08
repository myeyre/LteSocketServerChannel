package com.asiainfo.ocsp.lte.signal;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

/**
 * @since 2016.05.26
 * @author 宿荣全
 * @comment 机群版socket服务
 */
public class WorkNodeReceveTask implements Runnable {
	private Logger logger = Logger.getLogger(WorkNodeReceveTask.class);
	private LinkedBlockingQueue<Socket> lbkSocket = null;
	private LinkedBlockingQueue<byte[]> lbkAllMsg = null;
	private Socket socket = null;
	private ObjectInputStream is= null;

	public WorkNodeReceveTask(LinkedBlockingQueue<Socket> lbkSocket,LinkedBlockingQueue<byte[]> lbkAllMsg) {
		this.lbkSocket = lbkSocket;
		this.lbkAllMsg = lbkAllMsg;
	}
	public void run() {
		logger.info("WorkNodeReceveTask 正在启动！");
		
		byte[] msg = null;
		while (true) {

			try {
				socket = lbkSocket.take();
				is = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
				while (true){
					msg = (byte[]) is.readObject();
					lbkAllMsg.offer(msg);
				}
			} catch (IOException e) {
				logger.warn(e.getStackTrace());
			}catch (InterruptedException e) {
				logger.warn(e.getStackTrace());
			}catch (ClassNotFoundException e) {
				logger.warn(e.getStackTrace());
			} finally {
				logger.warn("socket client 已经断开！");
				try {
					if (is != null){
						is.close();
					}
					if (socket != null){
						socket.close();
					}
				} catch (IOException e) {
					logger.warn(e.getStackTrace());
				}
			}
		}
	}
}