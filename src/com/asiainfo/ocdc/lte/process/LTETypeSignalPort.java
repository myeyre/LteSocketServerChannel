package com.asiainfo.ocdc.lte.process;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;

import kafka.producer.KeyedMessage;

import org.apache.log4j.Logger;

/**
 * @since 2016.05.04
 * @author 宿荣全
 * @see 4G数据LTE的socket接收（信令端）
 */
public class LTETypeSignalPort implements Runnable {
	private  Logger logger = Logger.getLogger(LTETypeSignalPort.class);
	private LinkedBlockingQueue<byte[]> lbkAllMsg = null;
	private LinkedBlockingQueue<ArrayList<KeyedMessage<String, String>>> msg_queue = null;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private HashMap<String, String> topicMap = null;
	private Properties prop = null;
	// 
	private String itemSeparator = "|";

	public LTETypeSignalPort(
			LinkedBlockingQueue<byte[]> lbkAllMsg,
			LinkedBlockingQueue<ArrayList<KeyedMessage<String, String>>> msg_queue,
			Properties prop) {
		this.lbkAllMsg = lbkAllMsg;
		this.msg_queue = msg_queue;
		this.prop = prop;
	}

	// ［配置文件配置项］kafka异步发送数据包条数
	int uuSsendmsgNum = 0;
	int x2SsendmsgNum = 0;
	int uE_MRSsendmsgNum = 0;
	int cell_MRSsendmsgNum = 0;
	int s1_MMESsendmsgNum = 0;
	int s6aSsendmsgNum = 0;
	int s11SsendmsgNum = 0;
	int sGsSsendmsgNum = 0;
	boolean time_flg = false;
	private void setProp(){
		// ［配置文件配置项］kafka异步发送数据包条数
		uuSsendmsgNum = Integer.parseInt(prop.getProperty("socket.producer.sendmsg.uu.size").trim());
		x2SsendmsgNum = Integer.parseInt(prop.getProperty("socket.producer.sendmsg.x2.size").trim());
		uE_MRSsendmsgNum = Integer.parseInt(prop.getProperty("socket.producer.sendmsg.uE_MR.size").trim());
		cell_MRSsendmsgNum = Integer.parseInt(prop.getProperty("socket.producer.sendmsg.cell_MR.size").trim());
		s1_MMESsendmsgNum = Integer.parseInt(prop.getProperty("socket.producer.sendmsg.s1_MME.size").trim());
		s6aSsendmsgNum = Integer.parseInt(prop.getProperty("socket.producer.sendmsg.s6a.size").trim());
		s11SsendmsgNum = Integer.parseInt(prop.getProperty("socket.producer.sendmsg.s11.size").trim());
		sGsSsendmsgNum = Integer.parseInt(prop.getProperty("socket.producer.sendmsg.sGs.size").trim());
		// ［配置文件配置项］是否追加接收到信令时的时间戳
		time_flg = Boolean.valueOf(prop.getProperty("socket.lte.append.time.flg"));
	}

	private ArrayList<KeyedMessage<String, String>> uumsgList = new ArrayList<KeyedMessage<String, String>>();
	private ArrayList<KeyedMessage<String, String>> x2msgList = new ArrayList<KeyedMessage<String, String>>();
	private ArrayList<KeyedMessage<String, String>> uE_MRmsgList = new ArrayList<KeyedMessage<String, String>>();
	private ArrayList<KeyedMessage<String, String>> cell_MRmsgList = new ArrayList<KeyedMessage<String, String>>();
	private ArrayList<KeyedMessage<String, String>> s1_MMEmsgList = new ArrayList<KeyedMessage<String, String>>();
	private ArrayList<KeyedMessage<String, String>> s6amsgList = new ArrayList<KeyedMessage<String, String>>();
	private ArrayList<KeyedMessage<String, String>> s11msgList = new ArrayList<KeyedMessage<String, String>>();
	private ArrayList<KeyedMessage<String, String>> sGsmsgList = new ArrayList<KeyedMessage<String, String>>();
	
	public void run() {
		setProp();
		// topic map 解析
		topicMap = propAnalysis();
		// 解析信令并分类
		short messageType = 0;
		int sequenceId = 0;
		byte totalContents = 0;
		int totalLength = 0;
		byte[] buffer_lte = null;
		try {
			logger.info("多线程分类处理数据 LTETypeSignalPort启动成功！");
			while (true) {
				int off = 0;
				buffer_lte = lbkAllMsg.take();
				totalLength = buffer_lte.length + 2;
				messageType = ConvToByte.byteToShort(buffer_lte, off);
				off += 2;
				sequenceId = ConvToByte.byteToInt(buffer_lte, off);
				off += 4;
				totalContents = buffer_lte[off++];
				off += 1;
				// 　信令类型
				int signalType = buffer_lte[off + 4] & 0xff;
				// 单接口xdr，接口类型是5的场合，判断为s1-mme接口，否则记录到badmessage
				for (int i = 0; i < (totalContents & 0xff); i++) {
					// 　信令解析
					signalAnalysis(signalType, buffer_lte,off, totalContents, time_flg);
				}
			}
		} catch (Exception e) {
			logger.error("totalLength=" + totalLength + ";messageType="
					+ messageType + ";sequenceId=" + sequenceId
					+ ";totalContents=" + totalContents);
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @param signalType
	 * @param buffer_lte
	 * @param time_flg
	 */
	private void signalAnalysis(int signalType, byte[] buffer_lte,int off, byte totalContents, boolean time_flg) {

		switch (signalType) {
		 case 1:
		 // Uu_flg
		 uu_Analysis(buffer_lte,off,time_flg);
		 break;
		 case 2:
		 //X2_flg
		 X2Analysis(buffer_lte,off,time_flg);
		 break;
		 case 3:
		 //UE_MR_flg
		uE_MRAnalysis(buffer_lte,off,time_flg);
		 break;
		 case 4:
		 // Cell_MR_flg
		 cell_MRAnalysis(buffer_lte,off,time_flg);
		 break;
		case 5:
		// S1-MME_flg
		s1_MME_Analysis(buffer_lte,off,time_flg);
		break;
		 case 6:
		 // S6a_flg
		 sa6_Analysis(buffer_lte,off,time_flg);
		 break;
		 case 7:
		 //S11_flg
		 s11_Analysis(buffer_lte,off,time_flg);
		 break;
		 case 9:
		 // SGs_flg
		 sGs_Analysis(buffer_lte,off,time_flg);
	 break;
		default:
			logger.info("错误数据："+buffer_lte.toString());
			break;
		}
		buffer_lte = null;
	}
	
	/**
	 * Ｕｕ　解析 type =１
	 * @param buffer_lte
	 * @param totalContents
	 * @param time_print_flg
	 */
	private void uu_Analysis(byte[] buffer, int off, boolean time_print_flg) {
		StringBuilder sb = new StringBuilder();
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// Length
		off += 2;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 2, true)).append( "|");// City
		off += 2;
		sb.append(buffer[off] ).append( "|");// Interface
		off += 1;
		sb.append(ConvToByte.getHexString(buffer, off, off + 16)).append( "|");// XDR
		off += 16;
		sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append( "|");// RAT
		off += 1;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 8, true)).append( "|");// IMSI
		off += 8;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 8, true)).append( "|");// IMEI
		off += 8;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 16, false)).append( "|");// MSISDN
		off += 16;
		sb.append((buffer[off] & 0xff)).append( "|");// Procedure Type
		off += 1;
		sb.append(ConvToByte.byteToLong(buffer, off)).append( "|");// Procedure Start
		off += 8;
		sb.append(ConvToByte.byteToLong(buffer, off)).append( "|");// Procedure End
		off += 8;
		sb.append((buffer[off] & 0xff)).append( "|");// Keyword 1
		off += 1;
		sb.append((buffer[off] & 0xff)).append( "|");// Keyword 2
		off += 1;
		sb.append((buffer[off] & 0xff)).append( "|");// Procedure Status
		off += 1;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// PLMN ID
		off += 3;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// eNB ID
		off += 4;
		sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append( "|");// Cell ID
		off += 4;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// C-RNTI
		off += 2;
		sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append( "|");// Target eNB ID
		off += 4;
		sb.append(ConvToByte.getHexString(buffer, off, off + 4)).append( "|");// Target Cell ID
		off += 4;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// Target C-RNTI
		off += 2;
		sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append( "|");// MME UE 
		off += 4;
		sb.append(ConvToByte.byteToUnsignedShort(buffer,off)).append( "|");
		off += 2;
		sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append( "|");// MME Code
		off += 1;
		sb.deleteCharAt(sb.lastIndexOf(itemSeparator));
		
		String message= sb.toString();
		KeyedMessage<String, String> km = new KeyedMessage<String, String>(topicMap.get("1"),String.valueOf(message.hashCode()),message);
		// 如果消息队列满足sendmsgNum条向kafka队列缓存队列存储
		uumsgList.add(km);
		if (uumsgList.size() == uuSsendmsgNum) {
			msg_queue.offer(uumsgList);
			uumsgList = new ArrayList<KeyedMessage<String, String>>();
		}
		
	}
	/**
	 * ｘ２　解析 type =２
	 * @param buffer_lte
	 * @param totalContents
	 * @param time_print_flg
	 */
	private void X2Analysis(byte[] buffer_lte, int off, boolean time_print_flg) {
		
		StringBuilder sb = new StringBuilder();
		sb.append(ConvToByte.byteToUnsignedShort(buffer_lte, off)).append( "|");// Length
		off += 2;
		sb.append(ConvToByte.decodeTBCD(buffer_lte, off, off + 2, true)).append( "|");// City
		off += 2;
		sb.append(buffer_lte[off] ).append( "|");// Interface
		off += 1;
		sb.append(ConvToByte.getHexString(buffer_lte, off, off + 16)).append( "|");// XDR
																		// ID
		off += 16;
		sb.append(ConvToByte.byteToUnsignedByte(buffer_lte, off)).append( "|");// RAT
		off += 1;
		sb.append(ConvToByte.decodeTBCD(buffer_lte, off, off + 8, true)).append( "|");// IMSI
		off += 8;
		sb.append(ConvToByte.decodeTBCD(buffer_lte, off, off + 8, true)).append( "|");// IMEI
		off += 8;
		sb.append(ConvToByte.decodeTBCD(buffer_lte, off, off + 16, false)).append( "|");// MSISDN
		off += 16;
		sb.deleteCharAt(sb.lastIndexOf(itemSeparator));
		
		String message= sb.toString();
		KeyedMessage<String, String> km = new KeyedMessage<String, String>(topicMap.get("2"),String.valueOf(message.hashCode()),message);
		// 如果消息队列满足sendmsgNum条向kafka队列缓存队列存储
		x2msgList.add(km);
		if (x2msgList.size() == x2SsendmsgNum) {
			msg_queue.offer(x2msgList);
			x2msgList = new ArrayList<KeyedMessage<String, String>>();
		}
		
	}
	
	/**
	 * ＵE_MR　解析 type =3
	 * 
	 * @param buffer_lte
	 * @param time_print_flg
	 */
	private void uE_MRAnalysis(byte[] buffer,int off, boolean time_print_flg) {
		
		StringBuilder sb = new StringBuilder();
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// Length
		off += 2;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 2, true)).append( "|");// City
		off += 2;
		sb.append(buffer[off]).append( "|");// Interface
		off += 1;
		sb.append(ConvToByte.getHexString(buffer, off, off + 16)).append( "|");// XDR
																		// ID
		off += 16;
		sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append( "|");// RAT
		off += 1;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 8, true)).append( "|");// IMSI
		off += 8;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 8, true)).append( "|");// IMEI
		off += 8;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 16, false)).append( "|");// MSISDN
		off += 16;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// MME
																		// Group
																		// ID
		off += 2;
		sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append( "|");// MME Code
		off += 1;
		sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append( "|");// MME UE
																	// S1AP ID
		off += 4;
		sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append( "|");// eNB ID
		off += 4;
		sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append( "|");// Cell ID
		off += 4;
		sb.append(ConvToByte.byteToLong(buffer, off)).append( "|");// Time
		off += 8;
		sb.append((buffer[off] & 0xff)).append( "|");// MR type
		off += 1;
		sb.append((buffer[off] & 0xff)).append( "|");// PHR
		off += 1;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// eNB
																		// Received
																		// Power
		off += 2;
		sb.append((buffer[off] & 0xff)).append( "|");// UL SINR
		off += 1;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// TA
		off += 2;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// AoA
		off += 2;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// Serving
																		// Freq
		off += 2;
		sb.append((buffer[off] & 0xff)).append( "|");// Serving RSRP
		off += 1;
		sb.append((buffer[off] & 0xff)).append( "|");// Serving RSRQ
		off += 1;
		int neighborCellNum = ConvToByte.byteToUnsignedByte(buffer, off);
		sb.append(neighborCellNum + itemSeparator);// Neighbor Cell Number
		off += 1;
		// System.out.print("uemr sb1=============" + sb.toString());
		for (int n = 0; n < neighborCellNum; n++) {
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// Neighbor
																			// 1
																			// Cell
																			// PCI
			off += 2;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// Neighbor
																			// 1
																			// Freq
			off += 2;
			sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append( "|");// Neighbor
																		// 1
																		// RSRP
			off += 1;
			sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append( "|");// Neighbor
																		// 1
																		// RSRQ
			off += 1;
		}
		buffer = null;
		if (time_flg) {
			sb.append(sdf.format(System.currentTimeMillis()));
		}else {
			sb.deleteCharAt(sb.lastIndexOf(itemSeparator));
		}
		
		String message= sb.toString();
		KeyedMessage<String, String> km = new KeyedMessage<String, String>(topicMap.get("3"),String.valueOf(message.hashCode()),message);
		// 如果消息队列满足sendmsgNum条向kafka队列缓存队列存储
		uE_MRmsgList.add(km);
		if (uE_MRmsgList.size() == uE_MRSsendmsgNum) {
			msg_queue.offer(uE_MRmsgList);
			uE_MRmsgList = new ArrayList<KeyedMessage<String, String>>();
		}
		
	}

	/**
	 * Cell_MR　解析 type =4
	 * 
	 * @param buffer_lte
	 * @param time_print_flg
	 */
	private void cell_MRAnalysis(byte[] buffer, int off, boolean time_print_flg) {
		
		StringBuilder sb = new StringBuilder();
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// Length
		off += 2;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 2, true)).append( "|");// City
		off += 2;
		sb.append(buffer[off]).append( "|");// Interface
		off += 1;
		sb.append(ConvToByte.getHexString(buffer, off, off + 16)).append( "|");// XDR
																		// ID
		off += 16;
		sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append( "|");// RAT
		off += 1;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 8, true)).append( "|");// IMSI
		off += 8;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 8, true)).append( "|");// IMEI
		off += 8;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 16, false)).append( "|");// MSISDN
		off += 16;
		sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append( "|");// eNB ID
		off += 4;
		sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append( "|");// Cell ID
		off += 4;
		sb.append(ConvToByte.byteToLong(buffer, off)).append( "|");// Time
		off += 8;
		sb.append(ConvToByte.getHexString(buffer, off, 20)).append( "|");// eNB
																	// Received
																	// Interfere
		off += 20;
		sb.append(ConvToByte.getHexString(buffer, off, 9)).append( "|");// UL Packet
																	// Loss
		off += 9;
		sb.append(ConvToByte.getHexString(buffer, off, 9)).append( "|");// DL packet
		buffer = null;												 // Loss
		if (time_flg) {
			sb.append(sdf.format(System.currentTimeMillis()));
		}else {
			sb.deleteCharAt(sb.lastIndexOf(itemSeparator));
		}
		
		String message= sb.toString();
		KeyedMessage<String, String> km = new KeyedMessage<String, String>(topicMap.get("4"),String.valueOf(message.hashCode()),message);
		// 如果消息队列满足sendmsgNum条向kafka队列缓存队列存储
		cell_MRmsgList.add(km);
		if (cell_MRmsgList.size() == cell_MRSsendmsgNum) {
			msg_queue.offer(cell_MRmsgList);
			cell_MRmsgList = new ArrayList<KeyedMessage<String, String>>();
		}
		
	}

	/**
	 * Ｓ1_MME　解析　type =5
	 * @param buffer_lte
	 * @param totalContents 条数
	 * @param time_print_flg
	 */
	private void s1_MME_Analysis(byte[] buffer, int off, boolean time_print_flg) {
		long starttime=System.nanoTime();
		long starttimec=System.currentTimeMillis();
		StringBuilder sb = new StringBuilder();
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// Length
		off += 2;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 2, true)).append( "|");// City
		off += 2;
		sb.append(buffer[off]).append( "|");// Interface
		off += 1;
		sb.append(ConvToByte.getHexString(buffer, off, off + 16)).append( "|");// XDR
																		// ID
		off += 16;
		sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append( "|");// RAT
		off += 1;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 8, true)).append( "|");// IMSI
		off += 8;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 8, true)).append( "|");// IMEI
		off += 8;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 16, false)).append( "|");// MSISDN
		off += 16;
		sb.append((buffer[off] & 0xff)).append( "|");// Procedure Type
		off += 1;
		sb.append(ConvToByte.byteToLong(buffer, off)).append( "|");// Procedure Start
															// Time
		off += 8;
		sb.append(ConvToByte.byteToLong(buffer, off)).append( "|");// Procedure End
															// Time
		off += 8;
		sb.append((buffer[off] & 0xff)).append( "|");// Procedure Status
		off += 1;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// Request
																		// Cause
		off += 2;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// Failure
																		// Cause
		off += 2;
		sb.append((buffer[off] & 0xff)).append( "|");// Keyword 1
		off += 1;
		sb.append((buffer[off] & 0xff)).append( "|");// Keyword 2
		off += 1;
		sb.append((buffer[off] & 0xff)).append( "|");// Keyword 3
		off += 1;
		sb.append((buffer[off] & 0xff)).append( "|");// Keyword 4
		off += 1;
		sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append( "|");// MME UE
																	// S1AP ID
		off += 4;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// Old MME
																		// Group
																		// ID
		off += 2;
		sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append( "|");// Old MME
																	// Code
		off += 1;
		sb.append(ConvToByte.getHexString(buffer, off, off + 4)).append( "|");// Old
																		// M-TMSI
		off += 4;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// MME
																		// Group
																		// ID
		off += 2;
		sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append( "|");// MME Code
		off += 1;
		sb.append(ConvToByte.getHexString(buffer, off, off + 4)).append( "|");// M-TMSI
		off += 4;
		sb.append(ConvToByte.getHexString(buffer, off, off + 4)).append( "|");// TMSI
		off += 4;
		sb.append(ConvToByte.getIpv4(buffer, off)).append( "|");// USER_IPv4
		off += 4;
		sb.append(ConvToByte.getIpv6(buffer, off)).append( "|");// USER_IPv6
		off += 16;
		sb.append(ConvToByte.getIp(buffer, off)).append( "|");// MME IP Add
		off += 16;
		sb.append(ConvToByte.getIp(buffer, off)).append( "|");// eNB IP Add
		off += 16;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// MME Port
		off += 2;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// eNB Port
		off += 2;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// TAC
		off += 2;
		sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append( "|");// Cell ID
		off += 4;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// Other
																		// TAC
		off += 2;
		sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append( "|");// Other ECI
		off += 4;
		sb.append(new String(buffer, off, 32).trim()).append( "|");// APN
		off += 32;

		int epsBearerNum = ConvToByte.byteToUnsignedByte(buffer, off);
		sb.append(epsBearerNum + itemSeparator);// EPS Bearer Number
		off += 1;
		for (int n = 0; n < epsBearerNum; n++) {
			sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append( "|");// Bearer
																		// 1 ID
			off += 1;
			sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append( "|");// Bearer
																		// 1
																		// Type
			off += 1;
			sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append( "|");// Bearer
																		// 1 QCI
			off += 1;
			sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append( "|");// Bearer
																		// 1
																		// Status
			off += 1;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// Bearer
																			// 1
																			// Request
																			// Cause
			off += 2;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// Bearer
																			// 1
																			// Failure
																			// Cause
			off += 2;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append( "|");// Bearer
																		// 1 eNB
																		// GTP-TEID
			off += 4;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append( "|");// Bearer
																		// 1 SGW
																		// GTP-TEID
			off += 4;
		}
		buffer = null;
		if (time_flg) {
			sb.append(sdf.format(System.currentTimeMillis()));
		}else {
			sb.deleteCharAt(sb.lastIndexOf(itemSeparator));
		}
		long midlltime=System.nanoTime();
		long midlltimec=System.currentTimeMillis();
		String message= sb.toString();
		KeyedMessage<String, String> km = new KeyedMessage<String, String>(topicMap.get("5"),String.valueOf(message.hashCode()),message);
		// 如果消息队列满足sendmsgNum条向kafka队列缓存队列存储
		s1_MMEmsgList.add(km);
		if (s1_MMEmsgList.size() == s1_MMESsendmsgNum) {
			msg_queue.offer(s1_MMEmsgList);
			s1_MMEmsgList = new ArrayList<KeyedMessage<String, String>>();
		}
		long endtime=System.nanoTime();
		long endtimec=System.currentTimeMillis();
		System.out.println("[S1-mme|ms]T1："+(midlltimec-starttimec)+" | T2:"+(endtimec-midlltimec));
		System.out.println("[S1-mme|μs]T1："+(midlltime-starttime)+" | T2:"+(endtime-midlltime));
		
	}
	
	/**
	 * sa6　解析 type =6
	 * @param buffer_lte
	 * @param totalContents
	 * @param time_print_flg
	 */
	private void sa6_Analysis(byte[] buffer,int off, boolean time_print_flg) {
		
		StringBuilder sb = new StringBuilder();
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// Length
		off += 2;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 2, true)).append( "|");// City
		off += 2;
		sb.append(buffer[off]).append( "|");// Interface
		off += 1;
		sb.append(ConvToByte.getHexString(buffer, off, off + 16)).append( "|");// XDR
																		// ID
		off += 16;
		sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append( "|");// RAT
		off += 1;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 8, true)).append( "|");// IMSI
		off += 8;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 8, true)).append( "|");// IMEI
		off += 8;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 16, false)).append( "|");// MSISDN
		off += 16;
		sb.append((buffer[off] & 0xff)).append( "|");// Procedure Type
		off += 1;
		sb.append(ConvToByte.byteToLong(buffer, off)).append( "|");// Procedure Start
															// Time
		off += 8;
		sb.append(ConvToByte.byteToLong(buffer, off)).append( "|");// Procedure End
															// Time
		off += 8;
		sb.append((buffer[off] & 0xff)).append( "|");// Procedure Status
		off += 1;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// Cause
		off += 2;
		sb.append(ConvToByte.getIpv4(buffer, off)).append( "|");// USER_IPv4
		off += 4;
		sb.append(ConvToByte.getIpv6(buffer, off)).append( "|");// USER_IPv6
		off += 16;
		sb.append(ConvToByte.getIp(buffer, off)).append( "|");// MME Address
		off += 16;
		sb.append(ConvToByte.getIp(buffer, off)).append( "|");// HSS Address
		off += 16;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// MME Port
		off += 2;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// HSS Port
		off += 2;
		sb.append(ConvToByte.getHexString(buffer, off, 44)).append( "|");// Origin-Realm
		off += 44;
		sb.append(ConvToByte.getHexString(buffer, off, 44)).append( "|");// Destination-Realm
		off += 44;
		sb.append(ConvToByte.getHexString(buffer, off, 64)).append( "|");// Origin-Host
		off += 64;
		sb.append(ConvToByte.getHexString(buffer, off, 64)).append( "|");// Destination-Host
		off += 64;
		sb.append(ConvToByte.byteToUnsignedInt(buffer, 4)).append( "|");// Application-ID
		off += 4;
		sb.append((buffer[off] & 0xff)).append( "|");// Subscriber-Status
		off += 1;
		sb.append((buffer[off] & 0xff)).append( "|");// Access-Restriction-Data
		off += 1;
		buffer = null;
		if (time_flg) {
			sb.append(sdf.format(System.currentTimeMillis()));
		}else {
			sb.deleteCharAt(sb.lastIndexOf(itemSeparator));
		}
		
		String message= sb.toString();
		KeyedMessage<String, String> km = new KeyedMessage<String, String>(topicMap.get("6"),String.valueOf(message.hashCode()),message);
		// 如果消息队列满足sendmsgNum条向kafka队列缓存队列存储
		s6amsgList.add(km);
		if (s6amsgList.size() == s6aSsendmsgNum) {
			msg_queue.offer(s6amsgList);
			s6amsgList = new ArrayList<KeyedMessage<String, String>>();
		}
		
	}
	/**
	 * S11　解析 type =7
	 * @param buffer_lte
	 * @param totalContents
	 * @param time_print_flg
	 */
	private void s11_Analysis(byte[] buffer,int off, boolean time_print_flg) {
		
		StringBuilder sb = new StringBuilder();
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// Length
		off += 2;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 2, true)).append( "|");// City
		off += 2;
		sb.append(buffer[off]).append( "|");// Interface
		off += 1;
		sb.append(ConvToByte.getHexString(buffer, off, off + 16)).append( "|");// XDR
																		// ID
		off += 16;
		sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append( "|");// RAT
		off += 1;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 8, true)).append( "|");// IMSI
		off += 8;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 8, true)).append( "|");// IMEI
		off += 8;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 16, false)).append( "|");// MSISDN
		off += 16;
		sb.append((buffer[off] & 0xff)).append( "|");// Procedure Type
		off += 1;
		sb.append(ConvToByte.byteToLong(buffer, off)).append( "|");// Procedure Start
															// Time
		off += 8;
		sb.append(ConvToByte.byteToLong(buffer, off)).append( "|");// Procedure End
															// Time
		off += 8;
		sb.append((buffer[off] & 0xff)).append( "|");// Procedure Status
		off += 1;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// Failure
																		// Cause
		off += 2;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// Request
																		// Cause
		off += 2;
		sb.append(ConvToByte.getIpv4(buffer, off)).append( "|");// USER_IPv4
		off += 4;
		sb.append(ConvToByte.getIpv6(buffer, off)).append( "|");// USER_IPv6
		off += 16;
		sb.append(ConvToByte.getIp(buffer, off)).append( "|");// MME Address
		off += 16;
		sb.append(ConvToByte.getIp(buffer, off)).append( "|");// SGW/Old MME Address
		off += 16;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// MME Port
		off += 2;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// SGW/Old
																		// MME
																		// Port
		off += 2;
		sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append( "|");// MME
																	// Control
																	// TEID
		off += 4;
		sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append( "|");// Old MME
																	// /SGW
																	// Control
																	// TEID
		off += 4;
		// System.out.print("s11 sb1=============" + sb.toString());
		sb.append(ConvToByte.getHexString2(buffer, off, off + 32)).append( "|"); // APN
		// sb.append(ConvToByte.getHexString2(buffer, off, off + 32)).append( "|");
		// //APN
		// System.out.println("s11======"+sb.toString());
		// sb.append(new String(buffer,off,32).trim()+ itemSeparator);//APN
		// System.out.print("s11 sb2=============" + sb.toString());

		off += 32;
		int epsBearerNum = ConvToByte.byteToUnsignedByte(buffer, off);
		sb.append(epsBearerNum + itemSeparator);// EPS Bearer Number
		off += 1;
		for (int n = 0; n < epsBearerNum; n++) {
			sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append( "|");// Bearer
																		// 1 ID
			off += 1;
			sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append( "|");// Bearer
																		// 1
																		// Type
			off += 1;
			sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append( "|");// Bearer
																		// 1 QCI
			off += 1;
			sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append( "|");// Bearer
																		// 1
																		// Status
			off += 1;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append( "|");// Bearer
																		// 1 eNB
																		// GTP-TEID
			off += 4;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append( "|");// Bearer
																		// 1 SGW
																		// GTP-TEID
			off += 4;
		}
		buffer = null;
		if (time_flg) {
			sb.append(sdf.format(System.currentTimeMillis()));
		}else {
			sb.deleteCharAt(sb.lastIndexOf(itemSeparator));
		}
		
		String message= sb.toString();
		KeyedMessage<String, String> km = new KeyedMessage<String, String>(topicMap.get("7"),String.valueOf(message.hashCode()),message);
		// 如果消息队列满足sendmsgNum条向kafka队列缓存队列存储
		s11msgList.add(km);
		if (s11msgList.size() == s11SsendmsgNum) {
			msg_queue.offer(s11msgList);
			s11msgList = new ArrayList<KeyedMessage<String, String>>();
		}
		
	}
	
	/**
	 * SGｓ　解析 type =９
	 * @param buffer_lte
	 * @param totalContents
	 * @param time_print_flg
	 */
	private void sGs_Analysis(byte[] buffer, int off, boolean time_print_flg) {
		
		StringBuilder sb = new StringBuilder();
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// Length
		off += 2;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 2, true)).append( "|");// City
		off += 2;
		sb.append(buffer[off]).append( "|");// Interface
		off += 1;
		sb.append(ConvToByte.getHexString(buffer, off, off + 16)).append( "|");// XDR
																		// ID
		off += 16;
		sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append( "|");// RAT
		off += 1;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 8, true)).append( "|");// IMSI
		off += 8;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 8, true)).append( "|");// IMEI
		off += 8;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 16, false)).append( "|");// MSISDN
		off += 16;
		sb.append((buffer[off] & 0xff)).append( "|");// Procedure Type
		off += 1;
		sb.append(ConvToByte.byteToLong(buffer, off)).append( "|");// Procedure Start
															// Time
		off += 8;
		sb.append(ConvToByte.byteToLong(buffer, off)).append( "|");// Procedure End
															// Time
		off += 8;
		sb.append((buffer[off] & 0xff)).append( "|");// Procedure Status
		off += 1;
		sb.append((buffer[off] & 0xff)).append( "|");// Sgs Cause
		off += 1;
		sb.append((buffer[off] & 0xff)).append( "|");// Reject Cause
		off += 1;
		sb.append((buffer[off] & 0xff)).append( "|");// CP Cause
		off += 1;
		sb.append((buffer[off] & 0xff)).append( "|");// RP Cause
		off += 1;
		sb.append(ConvToByte.getIpv4(buffer, off)).append( "|");// USER_IPv4
		off += 4;
		sb.append(ConvToByte.getIpv6(buffer, off)).append( "|");// USER_IPv6
		off += 16;
		sb.append(ConvToByte.getIp(buffer, off)).append( "|");// MME IP Add
		off += 16;
		sb.append(ConvToByte.getIp(buffer, off)).append( "|");// MSC Server OP Add
		off += 16;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// MME Port
		off += 2;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// MSC
																		// Server
																		// Port
		off += 2;
		sb.append((buffer[off] & 0xff)).append( "|");// Service Indicator
		off += 1;
		sb.append(ConvToByte.getHexString(buffer, off, 55)).append( "|");// MME Name
		off += 55;
		sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append( "|");// TMSI
		off += 4;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// New LAC
		off += 2;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// Old LAC
		off += 2;
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append( "|");// TAC
		off += 2;
		sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append( "|");// Cell ID
		off += 4;
		sb.append(ConvToByte.getHexString(buffer, off, 24)).append( "|");// Calling ID
		off += 24;
		sb.append((buffer[off] & 0xff)).append( "|");// VLR Name Length
		off += 1;
		sb.append(ConvToByte.getHexString(buffer, off, buffer.length)).append( "|");
		buffer = null;
		if (time_flg) {
			sb.append(sdf.format(System.currentTimeMillis()));
		}else {
			sb.deleteCharAt(sb.lastIndexOf(itemSeparator));
		}
		
		String message= sb.toString();
		KeyedMessage<String, String> km = new KeyedMessage<String, String>(topicMap.get("9"),String.valueOf(message.hashCode()),message);
		// 如果消息队列满足sendmsgNum条向kafka队列缓存队列存储
		sGsmsgList.add(km);
		if (sGsmsgList.size() == sGsSsendmsgNum) {
			msg_queue.offer(sGsmsgList);
			sGsmsgList = new ArrayList<KeyedMessage<String, String>>();
		}
		
	}
	
	/**
	 * 根据接口型类ID取topic名字
	 * @return
	 */
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
}