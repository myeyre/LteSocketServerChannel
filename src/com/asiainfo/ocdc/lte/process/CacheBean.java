package com.asiainfo.ocdc.lte.process;

import java.nio.ByteBuffer;
/**
 * @since 2016.06.13
 * @author 宿荣全
 * 缓冲区管理 
 */
public class CacheBean {
	
	/* 接受数据缓冲区 */
	private ByteBuffer sendbuffer = null;
	/* 发送数据缓冲区 */
	private ByteBuffer receivebuffer = null;
	
	public ByteBuffer getReceivebuffer() {
		return receivebuffer;
	}
	public void setReceivebuffer(int cacheSize) {
		this.receivebuffer = ByteBuffer.allocate(cacheSize);
	}
	public ByteBuffer getSendbuffer() {
		return sendbuffer;
	}
	public void setSendbuffer(int cacheSize) {
		this.sendbuffer = ByteBuffer.allocate(cacheSize);;
	}
}
