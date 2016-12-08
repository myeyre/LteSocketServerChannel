  package com.asiainfo.ocdc.lte.process;

import java.io.File;
import java.io.FileOutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import kafka.producer.KeyedMessage;

/**
 * 解码后的数据保存成文件
 * @author songgx
 *
 */
public class LteSaveFile implements Runnable {
	
	private  Logger logger = Logger.getLogger(LteSendTask.class);
	private LinkedBlockingQueue<ArrayList<KeyedMessage<String, String>>> lbk = null;
	Properties props = null;
	
	public LteSaveFile(LinkedBlockingQueue<ArrayList<KeyedMessage<String, String>>> lbk, Properties props) {
		this.lbk = lbk;
		this.props = props;
	}
	
	public void run(){
		logger.info("LteSaveFile start! "+ Thread.currentThread().getId());
		// 设置配置属性
		ArrayList<KeyedMessage<String, String>> msgList = null;
		while (true) {
			try {
				msgList = lbk.take();
				//producer.send(msgList);
				
				Date nowTime = new Date(System.currentTimeMillis());
			    SimpleDateFormat sdFormatter = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
				String retStrFormatNowDate = sdFormatter.format(nowTime);
				
				Random random = new Random();
				int diskNo=random.nextInt(2);
				String path0="";
				if ((diskNo+1)-10 >= 0){
					path0 = "data"+(diskNo+1);
				}
				else
				{
					path0 = "data0"+(diskNo+1);
				}
				path0="data";
				logger.info("写文件："+"/"+path0+"/ochadoop/Lte"+retStrFormatNowDate+".data");
				File file = new File("/"+path0+"/ochadoop/Lte"+retStrFormatNowDate+".data");
				file.setWritable(true, false);
				FileOutputStream fos = null;
				if (file.exists()){
				    fos = new FileOutputStream(file, true);
				}
				else{
				    File parent = file.getParentFile();
				    if(parent!=null){
				        parent.mkdirs();
				    }else{
				        file.mkdir();
				    }
				    fos = new FileOutputStream(file);
				}
				for (KeyedMessage<String, String> msg:msgList)
				{
					fos.write(msg.toString().getBytes());
					fos.write("\r\n".getBytes());
				}
				fos.flush();
				fos.close();
			} catch (Exception e) {
				logger.error("LteSaveFile.java :FileOutputStream.write 失败！");
				e.printStackTrace();
			}
		}
	}
}
