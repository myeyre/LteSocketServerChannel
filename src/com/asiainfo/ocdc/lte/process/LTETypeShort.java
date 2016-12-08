package com.asiainfo.ocdc.lte.process;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.log4j.Logger;
import kafka.producer.KeyedMessage;

/**
 * @author 宿荣全
 */
public class LTETypeShort implements Runnable {
	private static Logger logger = Logger.getLogger(LTETypeShort.class);
	private LinkedBlockingQueue<byte[]> lbkAllMsg = null;
	private LinkedBlockingQueue<ArrayList<KeyedMessage<String, String>>> msg_queue = null;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	HashMap<String, String> topicMap = null;
	Properties prop = null;
	
	public LTETypeShort(
			LinkedBlockingQueue<byte[]> lbkAllMsg,LinkedBlockingQueue<ArrayList<KeyedMessage<String, String>>> msg_queue,Properties prop) {
		this.lbkAllMsg = lbkAllMsg;
		this.msg_queue = msg_queue;
		this.prop = prop;
	}
	
	public void run() {
		// topic map 解析
		topicMap = propAnalysis();
		// TODO SURQ
//		CountWriter.setProp(prop);
		
		// kafka异步发送数据包条数
		int sendmsgNum = Integer.parseInt(prop.getProperty( "socket.producer.sendmsg.size").trim());
		String s1_mme_topic = prop.getProperty("topic_s1_mme").trim();
		
		if (s1_mme_topic == null || s1_mme_topic.equals(""))
			s1_mme_topic = "topic_lte_s1_mme";
		
		// 接受多种数据类型
		String interface_type = prop.getProperty("socket.lte.interface_type");
		String[] typesList = interface_type.split(",");
		
		int typesNum = typesList.length;
		boolean s1_u_flg = false;
		boolean s1_mme_flg = false;
		
		switch (typesNum) {
		case 1:
			// 只有可能有两种类型信令：s1_u,s1_mme
			if (interface_type.equals("s1_u")) {
				s1_u_flg = true;
			} else {
				s1_mme_flg = true;
			}
			break;
		case 2:

			// 两种类型信令全部接收
			s1_u_flg = true;
			s1_mme_flg = true;
			break;
		default:
			break;
		}
		
		// 是否追加接收到信令时的时间戳
		boolean time_flg = Boolean.valueOf(prop.getProperty("socket.lte.append.time.flg"));
		// 解析信令并分类
		short messageType = 0;
		int sequenceId = 0;
		byte totalContents = 0;
		int totalLength = 0;
		StringBuilder sb = new StringBuilder();
		ArrayList<KeyedMessage<String, String>> msgList = new ArrayList<KeyedMessage<String, String>>();
		try {
			while (true) {
				int off = 0;
				byte[] buffer_lte = lbkAllMsg.take();
				totalLength = buffer_lte.length + 2;
				messageType = ConvToByte.byteToShort(buffer_lte, off);
				off += 2;
				sequenceId = ConvToByte.byteToInt(buffer_lte, off);
				off += 4;
				totalContents = buffer_lte[off++];
				// s1-u的场合，数据为明文，直接转换字符串
				if (s1_u_flg && "s1_u".equals(interface_type)) {
					String[] messages = new String(buffer_lte, off, buffer_lte.length
							- off).split("\\r\\n");
					// 获取服务器当前系统时间
					String date = sdf.format(System.currentTimeMillis());
					for (String message : messages) {
						String topicName = "";
						String[] fields = message.split("\\|");
						if (fields.length >= 19) {
							String appTypeCode = fields[18];
							topicName = topicMap.get(appTypeCode);
						} else {
							sb.delete(0, sb.length());
							sb.append("total_length=" + totalLength);
							sb.append("sequenceId=" + sequenceId);
							sb.append("totalContents=" + totalContents);
							sb.append("message=" + message + "\n");
							// TODO SURQ
//							CountWriter.writeerror(sb.toString());// 记录脏数据
							continue;
						}
						if (topicName == null || "".equals(topicName))
							topicName = prop.getProperty("topic_s1_u_default");
						if (time_flg) {
							message += "|" + date;
						}
						KeyedMessage<String, String> km = new KeyedMessage<String, String>(topicName, message);
						// 如果消息队列满足sendmsgNum条向kafka队列缓存队列存储
						if (msgList.size() == sendmsgNum) {
							msg_queue.offer(msgList);
							msgList = new ArrayList<KeyedMessage<String, String>>();
						}
						msgList.add(km);
					}
				}
				// s1-mme场合，数据为xdr，需要解析
				if (s1_mme_flg && "s1_mme".equals(interface_type)) {

					int xdrtype = (buffer_lte[off] & 0xff);// 单接口/合成接口xdr标识
					off += 1;
					// 单接口xdr，接口类型是5的场合，判断为s1-mme接口，否则记录到badmessage
					for (int i = 0; i < (totalContents & 0xff); i++) {
						String date = sdf.format(System.currentTimeMillis());
						String message = "";
						if (xdrtype == 2 && (buffer_lte[off + 4] & 0xff) == 5) {
							message = getMessages(buffer_lte, off, date, time_flg);
						} else {
							sb.delete(0, sb.length());
							sb.append("total_length=" + totalLength);
							sb.append("sequenceId=" + sequenceId);
							sb.append("totalContents=" + totalContents);
							sb.append("body=");
							for (int j = off; j < buffer_lte.length; j++) {
								sb.append(String.format("%02X", buffer_lte[i]));
							}
							sb.append("\n");
							// TODO SURQ
//							CountWriter.writeerror(sb.toString());// 记录脏数据
							break;
						}
						KeyedMessage<String, String> km = new KeyedMessage<String, String>(
								s1_mme_topic, message);
						// 如果消息队列满足sendmsgNum条向kafka队列缓存队列存储
						if (msgList.size() == sendmsgNum) {
							msg_queue.offer(msgList);
							msgList = new ArrayList<KeyedMessage<String, String>>();
						}
						msgList.add(km);
					}
				}
			}
		} catch (Exception e) {
			logger.error("totalLength=" + totalLength + ";messageType="
					+ messageType + ";sequenceId=" + sequenceId
					+ ";totalContents=" + totalContents);
			e.printStackTrace();
		}
	}
	
	
	private HashMap<String, String> propAnalysis() {
		HashMap<String, String> topicMap = new HashMap<String, String>();
		// msgType_100=topic_lte_common
		Set<Entry<Object, Object>> entrySet = prop.entrySet();
		Iterator<Entry<Object, Object>> it = entrySet.iterator();
		while (it.hasNext()) {
			Entry<Object, Object> kvEntry = it.next();
			String key = (String) kvEntry.getKey();
			if ((key.trim()).startsWith("msgType_")) {
				// value：装载topic 名字
				String value = (String) kvEntry.getValue();
				String[] keyvalue = key.split("_");
				// 100 --> topic_lte_common
				topicMap.put(keyvalue[1], value);
			}
		}
		return topicMap;
	}

	private String getMessages(byte[] buffer, int off, String date,
			boolean time_flg) {
		StringBuilder sb = new StringBuilder();
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off) + "|");// Length
		off += 2;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 2, true) + "|");// City
		off += 2;
		sb.append(buffer[off] + "|");// Interface
		off += 1;
		sb.append(ConvToByte.getHexString(buffer, off, off + 16) + "|");// XDR
																		// ID
		off += 16;
		sb.append(ConvToByte.byteToUnsignedByte(buffer, off) + "|");// RAT
		off += 1;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 8, true) + "|");// IMSI
		off += 8;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 8, true) + "|");// IMEI
		off += 8;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 16, false) + "|");// MSISDN
		off += 16;
		sb.append((buffer[off] & 0xff) + "|");// Procedure Type
		off += 1;
		sb.append(ConvToByte.byteToLong(buffer, off) + "|");// Procedure Start
															// Time
		off += 8;
		sb.append(ConvToByte.byteToLong(buffer, off) + "|");// Procedure End
															// Time
		off += 8;
		sb.append((buffer[off] & 0xff) + "|");// Procedure Status
		off += 1;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off) + "|");// Request
																		// Cause
		off += 2;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off) + "|");// Failure
																		// Cause
		off += 2;
		sb.append((buffer[off] & 0xff) + "|");// Keyword 1
		off += 1;
		sb.append((buffer[off] & 0xff) + "|");// Keyword 2
		off += 1;
		sb.append((buffer[off] & 0xff) + "|");// Keyword 3
		off += 1;
		sb.append((buffer[off] & 0xff) + "|");// Keyword 4
		off += 1;
		sb.append(ConvToByte.byteToUnsignedInt(buffer, off) + "|");// MME UE
																	// S1AP ID
		off += 4;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off) + "|");// Old MME
																		// Group
																		// ID
		off += 2;
		sb.append(ConvToByte.byteToUnsignedByte(buffer, off) + "|");// Old MME
																	// Code
		off += 1;
		sb.append(ConvToByte.getHexString(buffer, off, off + 4) + "|");// Old
																		// M-TMSI
		off += 4;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off) + "|");// MME
																		// Group
																		// ID
		off += 2;
		sb.append(ConvToByte.byteToUnsignedByte(buffer, off) + "|");// MME Code
		off += 1;
		sb.append(ConvToByte.getHexString(buffer, off, off + 4) + "|");// M-TMSI
		off += 4;
		sb.append(ConvToByte.getHexString(buffer, off, off + 4) + "|");// TMSI
		off += 4;
		sb.append(ConvToByte.getIpv4(buffer, off) + "|");// USER_IPv4
		off += 4;
		sb.append(ConvToByte.getIpv6(buffer, off) + "|");// USER_IPv6
		off += 16;
		sb.append(ConvToByte.getIp(buffer, off) + "|");// MME IP Add
		off += 16;
		sb.append(ConvToByte.getIp(buffer, off) + "|");// eNB IP Add
		off += 16;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off) + "|");// MME Port
		off += 2;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off) + "|");// eNB Port
		off += 2;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off) + "|");// TAC
		off += 2;
		sb.append(ConvToByte.byteToUnsignedInt(buffer, off) + "|");// Cell ID
		off += 4;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off) + "|");// Other
																		// TAC
		off += 2;
		sb.append(ConvToByte.byteToUnsignedInt(buffer, off) + "|");// Other ECI
		off += 4;
		sb.append(new String(buffer, off, 32).trim() + "|");// APN
		off += 32;

		int epsBearerNum = ConvToByte.byteToUnsignedByte(buffer, off);
		sb.append(epsBearerNum + "|");// EPS Bearer Number
		off += 1;
		for (int n = 0; n < epsBearerNum; n++) {
			sb.append(ConvToByte.byteToUnsignedByte(buffer, off) + "|");// Bearer
																		// 1 ID
			off += 1;
			sb.append(ConvToByte.byteToUnsignedByte(buffer, off) + "|");// Bearer
																		// 1
																		// Type
			off += 1;
			sb.append(ConvToByte.byteToUnsignedByte(buffer, off) + "|");// Bearer
																		// 1 QCI
			off += 1;
			sb.append(ConvToByte.byteToUnsignedByte(buffer, off) + "|");// Bearer
																		// 1
																		// Status
			off += 1;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off) + "|");// Bearer
																			// 1
																			// Request
																			// Cause
			off += 2;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off) + "|");// Bearer
																			// 1
																			// Failure
																			// Cause
			off += 2;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off) + "|");// Bearer
																		// 1 eNB
																		// GTP-TEID
			off += 4;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off) + "|");// Bearer
																		// 1 SGW
																		// GTP-TEID
			off += 4;
			if (time_flg) {
				sb.append(date + "|");
			}
		}
		sb.deleteCharAt(sb.lastIndexOf("|"));
		return sb.toString();
	}

}
