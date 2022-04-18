package lma.case1;

import org.apache.zookeeper.*;

import java.io.IOException;

public class DistributeServer {

	private String connectString = "hadoop001:2181,hadoop002:2181,hadoop003:2181";
	private int sessionTimeout = 2000;
	private ZooKeeper zk;

	/**
	 * 获取连接
	 * @throws IOException
	 */
	public void getConnection() throws IOException {
		zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			@Override
			public void process(WatchedEvent watchedEvent) {

			}
		});
	}

	/**
	 * 注册服务器
	 * @param hostName
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void registerServer(String hostName) throws InterruptedException, KeeperException {
		String newNode = zk.create("/servers/" + hostName, hostName.getBytes(),
				ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println(hostName + " is online");
	}

	/**
	 * 业务功能
	 * @param hostName
	 * @throws InterruptedException
	 */
	public void business(String hostName) throws InterruptedException {
		System.out.println(hostName + " is working");
		Thread.sleep(Long.MAX_VALUE);
	}

	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		DistributeServer server = new DistributeServer();
		server.getConnection();
		server.registerServer(args[0]);
		server.business(args[0]);
	}
}
