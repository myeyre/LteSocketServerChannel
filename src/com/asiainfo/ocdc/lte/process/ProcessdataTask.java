package com.asiainfo.ocdc.lte.process;

//import java.io.DataOutputStream;
import java.io.IOException;
//import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

public class ProcessdataTask implements Runnable {
	private static Logger logger = Logger.getLogger(ProcessdataTask.class);
	private LinkedBlockingQueue<SDTPDataBean> lbkSDTP = null;
	private LinkedBlockingQueue<byte[]> lbkAllMsg = null;
	private byte[] buffer = null;
	private SDTPDataBean stdpBean = null;
	
	public ProcessdataTask(LinkedBlockingQueue<SDTPDataBean> lbkSDTP,LinkedBlockingQueue<byte[]> lbkAllMsg) {
		this.lbkSDTP = lbkSDTP;
		this.lbkAllMsg = lbkAllMsg;
	}

	public void run() {
		try {
			processesData();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 解析socket数据
	 */
	private void processesData() throws IOException {
		
//		int sequenceId = 0;
//		byte totalContents = 0;
//		DataOutputStream out = null;
//		Socket socket = null;
		
		while (true){
			try {
				stdpBean = lbkSDTP.take();
				buffer = stdpBean.getBuffer();
//				socket = stdpBean.getSocket();
//				out = new DataOutputStream(socket.getOutputStream());
			} catch (InterruptedException e) {
				logger.error("processesData 方法 lbkSDTP.take() error!");
				e.printStackTrace();
			}
			
//			int off = 2;
//			// 3.交互的流水号，顺序累加，步长为1，循环使用（一个交互的一对请求和应答消息的流水号必须相同）
//			sequenceId = ConvToByte.byteToInt(buffer, off);
//			off += 4;
//			// 4.消息体中的事件数量（最多40条,若考虑实时性要求，可每次只填一个事件)
//			totalContents = buffer[off];
			
//			// 数据应答
//			byte[] requestArray = responseNotifyEventData(sequenceId,totalContents);
//			out.write(requestArray);
			// 将数据放入随机选择的处理线程的队列中
			lbkAllMsg.offer(buffer);
			// 记录数据包消息条数
//			out.close();
		}
	}
	
//	private byte[] responseNotifyEventData(int sequenceId, byte totalContents) {
//	short totalLength = 10;
//	int messageType = 0x8005;
//	byte reslut = 1;
//
//	byte[] totalLengthArray = ConvToByte.shortToByte(totalLength);
//	byte[] messageTypeArray = ConvToByte.intToByte(messageType);
//	byte[] sequenceIdArray = ConvToByte.intToByte(sequenceId);
//
//	byte[] requestArray = new byte[totalLength];
//	int pos = 0;
//	System.arraycopy(totalLengthArray, 0, requestArray, pos, 2);
//	pos += 2;
//	System.arraycopy(messageTypeArray, 2, requestArray, pos, 2);
//	pos += 2;
//	System.arraycopy(sequenceIdArray, 0, requestArray, pos, 4);
//	pos += 4;
//	requestArray[pos] = totalContents;
//	pos++;
//	requestArray[pos] = reslut;
//	return requestArray;
//	}
}