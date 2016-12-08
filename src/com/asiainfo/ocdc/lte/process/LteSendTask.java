package com.asiainfo.ocdc.lte.process;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class LteSendTask implements Runnable{
	
	private  Logger logger = Logger.getLogger(LteSendTask.class);
	private LinkedBlockingQueue<ArrayList<KeyedMessage<String, String>>> lbk = null;
	Properties props = null;
	
	public LteSendTask(LinkedBlockingQueue<ArrayList<KeyedMessage<String, String>>> lbk, Properties props) {
		this.lbk = lbk;
		this.props = props;
	}
	
	public void run(){
		logger.info("LteSendTask start! "+ Thread.currentThread().getId());
		// 设置配置属性
		ProducerConfig config = new ProducerConfig(props);
		// 创建producer
		Producer<String, String> producer = new Producer<String, String>(config);
		ArrayList<KeyedMessage<String, String>> msgList = null;
		while (true) {
			try {
				msgList = lbk.take();
				producer.send(msgList);
			} catch (InterruptedException e) {
				logger.error("LteSendTask.java :producer.send 失败！");
				e.printStackTrace();
			}
		}
	}
}