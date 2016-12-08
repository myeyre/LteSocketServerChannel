package com.asiainfo.ocdc.lte.process;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @since 2016.06.15
 * @author 宿荣全
 * @comment 机群版socket服务
 */
public class LteCacheServer {

	/* 缓冲区大小 */
	// 104857600（100M）
	private int BLOCK = 104857600;
	private Selector selector = null;
	
	public static ArrayList<SocketChannelProcess> countList = new ArrayList<SocketChannelProcess>();
	
	public LteCacheServer(int port) throws IOException {
		// 打开服务器套接字通道
		ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
		// 服务器配置为非阻塞
		serverSocketChannel.configureBlocking(false);
		// 检索与此通道关联的服务器套接字
		ServerSocket serverSocket = serverSocketChannel.socket();
		// 进行服务的绑定
		serverSocket.bind(new InetSocketAddress(port));
		// 开启Selector
		selector = Selector.open();
		// 注册到selector，等待连接
		serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
		System.out.println("ServerSocketChannel Start----:");
	}
	
	/**
	 * 监听各个信道
	 * @throws IOException
	 */
	public void listenAction(LinkedBlockingQueue<byte[]> lbkAllMsg) throws IOException {
		int channelID = 1;
		while (true) {
			// 选择一组键，并且相应的通道已经打开
			if (selector.select() == 0)
				continue;
			// 返回已选择键集
			Set<SelectionKey> selectionKeys = selector.selectedKeys();
			Iterator<SelectionKey> iterator = selectionKeys.iterator();
			while (iterator.hasNext()) {
				SelectionKey selectionKey = iterator.next();
				iterator.remove();

				if (selectionKey.isAcceptable()) {
					SocketChannel socketChannel = ((ServerSocketChannel) selectionKey.channel()).accept();
					socketChannel.configureBlocking(false);
					ChannelParamBean channelParamBean = new ChannelParamBean();
					channelParamBean.setRecevebuffer(BLOCK);
					channelParamBean.setSendbuffer(BLOCK);
					channelParamBean.setChannelID(channelID++);
					// 客户端信息
					channelParamBean.setHostInfo( "[" + socketChannel.socket().getInetAddress().getHostName() + "]");
					socketChannel.register(selector, SelectionKey.OP_READ,channelParamBean);
//					new Thread(new SocketChannelProcess(socketChannel,channelParamBean,lbkAllMsg)).start();
					// TODO DEBUG 统计数据量用 start
					SocketChannelProcess scp = new SocketChannelProcess(socketChannel,channelParamBean,lbkAllMsg);
					countList.add(scp);
					new Thread(scp).start();
					// TODO DEBUG 统计数据量用 end
				} else if (selectionKey.isReadable()) {
					// 取本信道的处理变量
					ChannelParamBean channelParam = (ChannelParamBean) selectionKey.attachment();
					while(true) {
						// 激活socketChannel去处理
						if (channelParam.isProcessed()){
							channelParam.setProcessed(false);
							channelParam.getChIndexQueue().offer(channelParam.getChannelID());
							break;
						}else {
							continue;
						}
					}
				}
			}
		}
	}
}