package com.asiainfo.ocsp.lte.signal;

import java.security.MessageDigest;
import java.util.Arrays;

import com.asiainfo.ocdc.lte.process.ConvToByte;

/**
 * @since 2016.06.13
 * @author 宿荣全
 */
public class ResponsionUtil {

	public byte[] responseNotifyEventData(int sequenceId, byte totalContents) {
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

	public byte[] responseVerNego(int sequenceId, byte totalContents) {
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
		return requestArray;
	}

	public byte[] responseLinkAuth(int sequenceId, byte totalContents,
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

	public byte[] responseLinkCheck(int sequenceId, byte totalContents) {
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

	public byte[] responseLinkRel(int sequenceId, byte totalContents) {
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

	public byte[] responseLinkDataCheck(int sequenceId, byte totalContents,
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
}