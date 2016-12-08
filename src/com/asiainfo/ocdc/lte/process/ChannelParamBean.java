package com.asiainfo.ocdc.lte.process;

import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @since 2016.06.15
 * @author 宿荣全
 * @comment 机群版socket服务
 */
public class ChannelParamBean {
	private ByteBuffer sendbuffer = null;
	private ByteBuffer recevebuffer = null;
	private volatile boolean processed= true;
	private int channelID = 0;
	private String hostInfo = null;
	
	//  DEBUG 统计接收总条数 start
	private long totalCount = 0l;
	public void addTotalCount(long subCount) {
		totalCount = totalCount + subCount;
	}
	public long getTotalCount() {
		return totalCount;
	}
	//  DEBUG 统计接收总条数 end
	public LinkedBlockingQueue<Integer> ChIndexQueue = new LinkedBlockingQueue<Integer>();
	
	public ByteBuffer getSendbuffer() {
		return sendbuffer;
	}
	public void setSendbuffer(int size) {
		this.sendbuffer = ByteBuffer.allocate(size);
	}
	public ByteBuffer getRecevebuffer() {
		return recevebuffer;
	}
	public void setRecevebuffer(int size) {
		this.recevebuffer = ByteBuffer.allocate(size);
	}
	public boolean isProcessed() {
		synchronized(this) {
			return processed;
		}
	}
	public void setProcessed(boolean processed) {
		this.processed = processed;
	}

	public int getChannelID() {
		return channelID;
	}
	public void setChannelID(int channelID) {
		this.channelID = channelID;
	}
	
	public String getHostInfo() {
		return hostInfo;
	}
	public void setHostInfo(String hostInfo) {
		this.hostInfo = hostInfo;
	}
	
	public LinkedBlockingQueue<Integer> getChIndexQueue() {
		return ChIndexQueue;
	}


}