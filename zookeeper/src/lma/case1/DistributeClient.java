package lma.case1;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class DistributeClient {

	private String connectString = "hadoop001:2181,hadoop002:2181,hadoop003:2181";
	private int sessionTimeout = 2000;
	private String parentNode = "/servers";
	private ZooKeeper zk;

	/**
	 * 获取连接
	 * @throws IOException
	 */
	public void getConnection() throws IOException {
		zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			@Override
			public void process(WatchedEvent watchedEvent) {
				try {
					getServerList();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * 获取服务器列表
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void getServerList() throws InterruptedException, KeeperException {
		List<String> children = zk.getChildren(parentNode, true);
		ArrayList<String> serverList = new ArrayList<>();
		for (String child : children) {
			byte[] data = zk.getData(parentNode + "/" + child, false, null);
			serverList.add(new String(data));
		}
		System.out.println(serverList);
	}


	/**
	 * 业务功能
	 * @throws InterruptedException
	 */
	public void business() throws InterruptedException {
		Thread.sleep(Long.MAX_VALUE);
	}


	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		DistributeClient client = new DistributeClient();
		client.getConnection();
		client.getServerList();
		client.business();
	}

}
