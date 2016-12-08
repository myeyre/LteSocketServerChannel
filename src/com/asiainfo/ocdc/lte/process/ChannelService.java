package com.asiainfo.ocdc.lte.process;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
/**
 * @since 2016.06.13
 * @author 宿荣全
 */
public class ChannelService {

	/**
	 * 获取服务器信道
	 * @param port
	 * @return ServerSocketChannel
	 * @throws IOException
	 */
	public ServerSocketChannel getServerSocketChannel(int port) throws IOException {
		// 打开服务器套接字通道
		ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
		// 服务器配置为非阻塞
		serverSocketChannel.configureBlocking(false);
		// 检索与此通道关联的服务器套接字
		ServerSocket serverSocket = serverSocketChannel.socket();
		// 进行服务的绑定
		serverSocket.bind(new InetSocketAddress(port));
		return serverSocketChannel;
	}
	
	/**
	 * 获取监听器
	 * @param ssc
	 * @return Selector
	 * @throws IOException
	 */
	public Selector getSelector (ServerSocketChannel ssc) throws IOException{
		// 通过open()方法找到Selector
		Selector selector = Selector.open();
		// 注册到selector，等待连接
		ssc.register(selector, SelectionKey.OP_ACCEPT);
		return selector;
	}
}