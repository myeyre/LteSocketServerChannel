package com.asiainfo.ocdc.lte.process;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

public class NIOserverss {

	private static Selector selector = null;

	public static void main(String[] args) throws InterruptedException {
		 // ---------------------多客户端----------------------------------------
		try {
			// 打开监听信道
			ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
			// 与本地端口绑定
			serverSocketChannel.socket().bind(new InetSocketAddress(10112));
			// 设置为非阻塞模式
			serverSocketChannel.configureBlocking(false);
			// ByteBuffer sendbuffer = ByteBuffer.allocate(1024);
			// 创建选择器
			selector = Selector.open();
			// 将选择器绑定到监听信道,只有非阻塞信道才可以注册选择器.并在注册过程中指出该信道可以进行Accept操作
			// // 注册到selector，等待连接
			serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
			System.out.println("Server Start----:");

			int channelID = 1;
			// 反复循环,等待IO
			while (true) {
				// 等待某信道就绪(或超时(论循探测时间)) 选择一组键，并且相应的通道已经打开
				if (selector.select() == 0) {
					System.out.println("等待客户端信道就绪！");
					continue;
				}
				// 取得迭代器.selectedKeys()中包含了每个准备好某一I/O操作的信道的SelectionKey
				Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
				while (iterator.hasNext()) {
					SelectionKey key = iterator.next();
					// 移除处理过的键
					iterator.remove();

					if (key.isAcceptable()) {
						// 有客户端连接请求时
						SocketChannel socketChannel = ((ServerSocketChannel) key.channel()).accept();
						socketChannel.configureBlocking(false);
						ChannelParamBean channelParamBean = new ChannelParamBean();
						channelParamBean.setRecevebuffer(1024);
						channelParamBean.setSendbuffer(1024);
						channelParamBean.setChannelID(channelID++);
						socketChannel.register(selector,  SelectionKey.OP_READ,channelParamBean);
						new Thread(new SocketChannelProcessTest(socketChannel,channelParamBean)).start();
					}
					if (key.isReadable()) {
						ChannelParamBean channelParam = (ChannelParamBean) key.attachment();
						while(true) {
							if (channelParam.isProcessed()){
								// 激活socketChannel去处理,【注意跟下一句的顺序】
								channelParam.setProcessed(false);
								channelParam.getChIndexQueue().offer(channelParam.getChannelID());
								break;
							}else {
								continue;
							}
						}
						 System.out.println("---------等待客户端信道就绪！");
					}
//					if ( key.isWritable()) {
						if (key.isValid() && key.isWritable()) {
//						 客户端可写时
//						 获得与客户端通信的信道
//						 SocketChannel clientChannel = (SocketChannel)
//						 key.channel();
//						 // 得到并清空缓冲区
//						 ByteBuffer buffer = (ByteBuffer) key.attachment();
//						 System.out.println("等待客户端信道就绪！");
					}

				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

class SocketChannelProcessTest implements Runnable {
	private SocketChannel clientChannel = null;
	private ChannelParamBean channelParamBean = null;
	public SocketChannelProcessTest(SocketChannel socketChannel,ChannelParamBean channelParamBean) {
		this.clientChannel = socketChannel;
		this.channelParamBean = channelParamBean;
	}

	public void run() {
		while (true) {
			try {
				// 有数据
				channelParamBean.getChIndexQueue().take();
				// 获取信道固有缓存
				ByteBuffer receveBuffer = channelParamBean.getRecevebuffer();
				receveBuffer.clear();
				// 读取信息字节数
				long bytesRead = clientChannel.read(receveBuffer);
				System.out.println(channelParamBean.isProcessed());
				if (bytesRead == -1) {
					System.out.println("客户端socket关闭！");
					clientChannel.close();
				} else if (bytesRead > 1) {
					receveBuffer.flip();
					byte[] bt = new byte[receveBuffer.limit()];
					receveBuffer.get(bt);
					System.out.println("ChannelID:" + channelParamBean.getChannelID() + "context:" + new String(bt));
					ByteBuffer sendbuffer = channelParamBean.getSendbuffer();
					sendbuffer.clear();
					String result = channelParamBean.getChannelID() + "shoudao!";
					sendbuffer.put(result.getBytes());
					sendbuffer.flip();
					clientChannel.write(sendbuffer);
				}
				channelParamBean.setProcessed(true);
				System.out.println(channelParamBean.isProcessed());
			
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}