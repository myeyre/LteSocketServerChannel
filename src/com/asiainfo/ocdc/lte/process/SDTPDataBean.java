package com.asiainfo.ocdc.lte.process;

//import java.io.DataInputStream;
//import java.io.DataOutputStream;
import java.net.Socket;

/**
 * SDTP 协议bean
 * @author 宿荣全
 *
 */
public class SDTPDataBean {

	protected Socket socket = null;
	

	protected byte[] buffer = null;
//	protected DataInputStream dis = null;
//	protected DataOutputStream dos = null;
	
	public byte[] getBuffer() {
		return buffer;
	}
	public void setBuffer(byte[] buffer) {
		this.buffer = buffer;
	}
	
//	public DataInputStream getDis() {
//		return dis;
//	}
//	public void setDis(DataInputStream dis) {
//		this.dis = dis;
//	}
//	public DataOutputStream getDos() {
//		return dos;
//	}
//	public void setDos(DataOutputStream dos) {
//		this.dos = dos;
//	}
	
	public Socket getSocket() {
		return socket;
	}
	public void setSocket(Socket socket) {
		this.socket = socket;
	}
}