package lma.zookeepertest;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class ZookeeperTest {

	private String connectString = "hadoop001:2181,hadoop002:2181,hadoop003:2181";
	private int sessionTimeout = 2000;

	private ZooKeeper zkClient;

	@Before
	public void init() throws IOException {
		zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			@Override
			public void process(WatchedEvent watchedEvent) {
				System.out.println(watchedEvent.getType() + "--" + watchedEvent.getPath());
				try {
					List<String> children = zkClient.getChildren("/", true);
					for (String child : children) {
						System.out.println(child);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	@Test
	public void create() throws InterruptedException, KeeperException {
		String nodeCreated = zkClient.create("/atguigu", "meinv".getBytes(),
				ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}


	@Test
	public void getChildren() throws InterruptedException, KeeperException {
		List<String> children = zkClient.getChildren("/", true);
		for (String child: children) {
			System.out.println(child);
		}

		Thread.sleep(Long.MAX_VALUE);
	}

	@Test
	public void exist() throws InterruptedException, KeeperException {
		Stat exists = zkClient.exists("/zookeeper", true);
		System.out.println(exists == null ? "not exist" : "exist");
	}
}
