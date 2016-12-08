package com.asiainfo.ocdc.lte.process;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class ttttt {

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * 
	 */

	public static void main(String[] args) throws Exception {
		
		
		
		for (int i = 0;i<3; i++) {
			new Thread(new TestTask("thread"+i)).start();
		}
		
	}
}

class TestTask implements Runnable {
	
	private String name = "";
	public TestTask(String name) {
		this.name = name;
	}

	public void run() {
		Socket client = null;
		DataOutputStream out = null;
		DataInputStream is = null;
		try {
			client = new Socket("localhost", 10112);
			out = new DataOutputStream(client.getOutputStream());
			is = new DataInputStream(client.getInputStream());

			while (true) {
				
				for (int i = 0; i < 2; i++) {
					String wd = name+"write:"+i+"|";
					out.write(wd.getBytes());
					out.flush();
				}
				
			byte [] aa = new byte[9];
			is.read(aa);
			System.out.println(new String(aa));
			}
//			out.close();
//			client.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
//	public void run() {
//		
//		Socket client = null;
//		ObjectOutputStream out = null;
//		try {
//			client = new Socket("localhost", 10112);
//			out = new ObjectOutputStream(client.getOutputStream());
//			
//			while (true){
//				out.writeObject("su rong quan text !");
//				out.flush();
//				out.reset();
//				Thread.currentThread().sleep(1000);
//			}
//			
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		
//	}
}