package com.asiainfo.ocdc.lte.process;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

public class Processdata implements Runnable {
	private Logger logger = Logger.getLogger(Processdata.class);
	private LinkedBlockingQueue<Socket> lbkSocket = null;
	private LinkedBlockingQueue<byte[]> lbkAllMsg = null;
	private Socket socket = null;

	public Processdata(LinkedBlockingQueue<Socket> lbkSocket,
			LinkedBlockingQueue<byte[]> lbkAllMsg) {
		this.lbkSocket = lbkSocket;
		this.lbkAllMsg = lbkAllMsg;
	}

	public void run() {
		while (true) {
			try {
				socket = lbkSocket.take();
			} catch (InterruptedException e) {
				e.printStackTrace();
				logger.error("lbkSocket.take() error!");
			}
			try {
				if (socket != null){
					processData(socket);
				}
			} catch (Exception e) {
				logger.error("Processdata.java ---: 从lbkSocket 队列取值出错！");
				e.printStackTrace();
			}
		}
	}

	/**
	 * 解析socket数据
	 */
	private void processData(Socket socket) throws IOException {
		DataInputStream dis = new DataInputStream(socket.getInputStream());
		DataOutputStream out = new DataOutputStream(socket.getOutputStream());

		short messageType = 0;
		int sequenceId = 0;
		byte totalContents = 0;
		int totalLength = 0;
		boolean is_login = false;
		long receiveNum = 0L;
		// long receiveTotalNum = 0L;
		// long receiveXdrNum = 0L;
		// 设置关闭时不立即关闭连接，等数据发送完成或3秒后再关闭
		socket.setSoLinger(true, 3);
		do {
			totalLength = dis.readUnsignedShort();
			int length = totalLength - 2;
			byte[] buffer = new byte[length];
			int readnum = 0;
			while (readnum < length) {
				int num = dis.read(buffer, readnum, length - readnum);
				if (num > 0) {
					readnum += num;
				}
			}
			int off = 0;
			messageType = ConvToByte.byteToShort(buffer, off);
			off += 2;

			sequenceId = ConvToByte.byteToInt(buffer, off);
			off += 4;
			totalContents = buffer[off++];
			if (is_login && messageType == 5) {
				// sdtp数据校验用
				receiveNum++;
				// 记录接收的xdr数
				// receiveXdrNum++;
				// receiveTotalNum +=totalContents & 0xff;
				// 数据应答
				byte[] requestArray = responseNotifyEventData(sequenceId,
						totalContents);
				out.write(requestArray);
				// 将数据放入随机选择的处理线程的队列中
				lbkAllMsg.offer(buffer);
				// 记录数据包消息条数
			} else if (messageType == 1) {// verNego 版本协商
				byte[] requestArray = responseVerNego(sequenceId, totalContents);
				out.write(requestArray);
				out.flush();
			} else if (messageType == 2) {// linkAuth 权限验证
				byte[] loginId = new byte[12];
				byte[] digestArray = new byte[16];
				byte[] timestamp = new byte[4];
				byte[] rand = new byte[2];
				System.arraycopy(buffer, off, loginId, 0, 12);
				off += 12;
				System.arraycopy(buffer, off, digestArray, 0, 16);
				off += 16;
				System.arraycopy(buffer, off, timestamp, 0, 4);
				off += 4;
				System.arraycopy(buffer, off, rand, 0, 2);

				byte[] requestArray = responseLinkAuth(sequenceId,
						totalContents, loginId, digestArray, timestamp, rand);
				out.write(requestArray);
				out.flush();

				is_login = true;
			} else if (is_login && messageType == 3) {// linkCheck 链路检测
				byte[] requestArray = responseLinkCheck(sequenceId,
						totalContents);
				out.write(requestArray);
				out.flush();
			} else if (is_login && messageType == 7) {// linkDataCheck 链路数据发送校验
				int sendflag = ConvToByte.byteToInt(buffer, off);
				off += 4;
				int sendDataInfo = ConvToByte.byteToInt(buffer, off);

				byte[] requestArray = responseLinkDataCheck(sequenceId,
						totalContents, sendflag, sendDataInfo, (int) receiveNum);
				receiveNum = 0;
				out.write(requestArray);
				out.flush();
			} else {
				break;// 释放链接，未验证权限或者messageType不是1，2，3, 7
			}
		} while (messageType != 4);// linkRel 连接释放
		byte[] requestArray = responseLinkRel(sequenceId, totalContents);
		out.write(requestArray);
		out.flush();
		dis.close();
	}

	private byte[] responseNotifyEventData(int sequenceId, byte totalContents) {
		short totalLength = 10;
		int messageType = 0x8005;
		byte reslut = 1;

		byte[] totalLengthArray = ConvToByte.shortToByte(totalLength);
		byte[] messageTypeArray = ConvToByte.intToByte(messageType);
		byte[] sequenceIdArray = ConvToByte.intToByte(sequenceId);

		byte[] requestArray = new byte[totalLength];
		int pos = 0;
		System.arraycopy(totalLengthArray, 0, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(messageTypeArray, 2, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(sequenceIdArray, 0, requestArray, pos, 4);
		pos += 4;
		requestArray[pos] = totalContents;
		pos++;
		requestArray[pos] = reslut;

		return requestArray;
	}

	private byte[] responseLinkDataCheck(int sequenceId, byte totalContents,
			int sendflag, int sendDataInfo, int receiveDatainfo) {
		short totalLength = 22;
		int messageType = 0x8007;
		byte reslut = 0;

		byte[] totalLengthArray = ConvToByte.shortToByte(totalLength);
		byte[] messageTypeArray = ConvToByte.intToByte(messageType);
		byte[] sequenceIdArray = ConvToByte.intToByte(sequenceId);
		byte[] sendflagArray = ConvToByte.intToByte(sendflag);
		byte[] sendDataInfoArray = ConvToByte.intToByte(sendDataInfo);
		byte[] recciveDatainfoArray = ConvToByte.intToByte(receiveDatainfo);

		if (sendDataInfo == receiveDatainfo) {
			reslut = 0;
		} else if (sendDataInfo > receiveDatainfo) {
			reslut = 1;
		} else if (sendDataInfo < receiveDatainfo) {
			reslut = 2;
		}

		byte[] requestArray = new byte[totalLength];
		int pos = 0;
		System.arraycopy(totalLengthArray, 0, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(messageTypeArray, 2, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(sequenceIdArray, 0, requestArray, pos, 4);
		pos += 4;
		requestArray[pos] = totalContents;
		pos++;
		System.arraycopy(sendflagArray, 0, requestArray, pos, 4);
		pos += 4;
		requestArray[pos] = reslut;
		pos++;
		System.arraycopy(sendDataInfoArray, 0, requestArray, pos, 4);
		pos += 4;
		System.arraycopy(recciveDatainfoArray, 0, requestArray, pos, 4);
		return requestArray;
	}

	private byte[] responseVerNego(int sequenceId, byte totalContents) {
		short totalLength = 10;
		int messageType = 0x8001;
		byte reslut = 1;

		byte[] totalLengthArray = ConvToByte.shortToByte(totalLength);
		byte[] messageTypeArray = ConvToByte.intToByte(messageType);
		byte[] sequenceIdArray = ConvToByte.intToByte(sequenceId);

		byte[] requestArray = new byte[totalLength];
		int pos = 0;
		System.arraycopy(totalLengthArray, 0, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(messageTypeArray, 2, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(sequenceIdArray, 0, requestArray, pos, 4);
		pos += 4;
		requestArray[pos] = totalContents;
		pos++;
		requestArray[pos] = reslut;
		System.out.println("VerNego :" + reslut);
		return requestArray;
	}

	private byte[] responseLinkAuth(int sequenceId, byte totalContents,
			byte[] loginId, byte[] digestArray, byte[] timestamp, byte[] rand) {
		short totalLength = 74;
		int messageType = 0x8002;
		byte reslut = 0;
		String passwd = "asiainfo123";

		byte[] totalLengthArray = ConvToByte.shortToByte(totalLength);
		byte[] messageTypeArray = ConvToByte.intToByte(messageType);
		byte[] sequenceIdArray = ConvToByte.intToByte(sequenceId);

		int pos = 0;
		try {
			// 获取MD5编码器
			MessageDigest md = MessageDigest.getInstance("SHA-256");
			// 计算密码对应的SHA256编码值
			byte[] sha_passwd = md.digest(passwd.getBytes());
			StringBuilder sb = new StringBuilder();
			for (byte b : sha_passwd)
				sb.append(String.format("%02x", b));
			// 拼接临时字段，以计算digest，和客户端发送的值比对

			String str_tmp = (new String(loginId)) + sb.toString()
					+ ConvToByte.byteToInt(timestamp, 0) + "rand="
					+ ConvToByte.byteToShort(rand, 0);

			// 计算digest
			byte[] digest = md.digest(str_tmp.getBytes());
			// 比对计算出的digest和客户端发送过来的digest是否相等，相等返回1，否则返回0
			reslut = Arrays.equals(digest, digestArray) ? (byte) 1 : (byte) 0;

			sb.delete(0, sb.length());
			for (byte b : digestArray)
				sb.append(String.format("%02x", b));
			sb.delete(0, sb.length());
			for (byte b : digest)
				sb.append(String.format("%02x", b));

			reslut = 1;// 屏蔽验证，测试数据接收
		} catch (Exception e) {
			e.printStackTrace();
		}

		byte[] requestArray = new byte[totalLength];
		pos = 0;
		System.arraycopy(totalLengthArray, 0, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(messageTypeArray, 2, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(sequenceIdArray, 0, requestArray, pos, 4);
		pos += 4;
		requestArray[pos] = totalContents;
		pos++;
		requestArray[pos] = reslut;
		pos++;
		System.arraycopy(digestArray, 0, requestArray, pos, digestArray.length);

		return requestArray;
	}

	private byte[] responseLinkCheck(int sequenceId, byte totalContents) {
		short totalLength = 9;
		int messageType = 0x8003;

		byte[] totalLengthArray = ConvToByte.shortToByte(totalLength);
		byte[] messageTypeArray = ConvToByte.intToByte(messageType);
		byte[] sequenceIdArray = ConvToByte.intToByte(sequenceId);

		byte[] requestArray = new byte[totalLength];
		int pos = 0;
		System.arraycopy(totalLengthArray, 0, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(messageTypeArray, 2, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(sequenceIdArray, 0, requestArray, pos, 4);
		pos += 4;
		requestArray[pos] = totalContents;

		System.out.println("LinkCheck ");
		return requestArray;
	}

	private byte[] responseLinkRel(int sequenceId, byte totalContents) {
		short totalLength = 10;
		int messageType = 0x8004;
		byte reslut = 1;

		byte[] totalLengthArray = ConvToByte.shortToByte(totalLength);
		byte[] messageTypeArray = ConvToByte.intToByte(messageType);
		byte[] sequenceIdArray = ConvToByte.intToByte(sequenceId);

		byte[] requestArray = new byte[totalLength];
		int pos = 0;
		System.arraycopy(totalLengthArray, 0, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(messageTypeArray, 2, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(sequenceIdArray, 0, requestArray, pos, 4);
		pos += 4;
		requestArray[pos] = totalContents;
		pos++;
		requestArray[pos] = reslut;
		return requestArray;
	}
}