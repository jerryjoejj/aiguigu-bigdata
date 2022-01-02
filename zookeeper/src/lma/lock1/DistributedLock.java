package lma.lock1;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class DistributedLock {

	private final String connectString = "hadoop001:2181,hadoop002:2181,hadoop003:2181";
	private final int sessionTimeout = 2000;

	private final CountDownLatch connectLatch = new CountDownLatch(1);
	private final CountDownLatch preNodeLatch = new CountDownLatch(1);

	private final String rootNode = "/locks";
	private final String subNodePrefix = "/seq-";

	private ZooKeeper zk;
	private String currentNode;
	private String preNode;


	/**
	 * 创建类是创建连接
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public DistributedLock() throws IOException, InterruptedException, KeeperException {
		zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			@Override
			public void process(WatchedEvent watchedEvent) {
				// 连接建立，唤醒线程
				if (watchedEvent.getState() == Watcher.Event.KeeperState.SyncConnected) {
					connectLatch.countDown();
				}
				// 节点删除，唤醒等待线程
				if (watchedEvent.getType() == Event.EventType.NodeDeleted
						&& watchedEvent.getPath().equals(preNode)) {
					preNodeLatch.countDown();
				}
			}
		});

		connectLatch.await();
		Stat stat = zk.exists(this.rootNode, false);
		if (null == stat) {
			// 根节点不存在，创建节点
			System.out.println("根节点不存在");
			zk.create(rootNode, new byte[0],
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
	}

	/**
	 * 获取锁
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void zkLock() throws InterruptedException, KeeperException {
		// 创建节点获取锁
		currentNode = zk.create(rootNode + subNodePrefix, null,
				ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

		List<String> children = zk.getChildren(rootNode, false);
		if (children.size() == 1) {
			// 如果列表中只有一个节点，直接获得锁
			return;
		} else {
			// 对节点列表进行排序
			Collections.sort(children);
			// 获取当前节点的名称
			String thisNode = currentNode.substring((rootNode + "/").length());
			// 获取当前节点的位置
			int thisNodeIndex = children.indexOf(thisNode);
			if (thisNodeIndex == -1) {
				throw new RuntimeException("数据异常");
			} else if (thisNodeIndex == 0) {
				// thisNode在节点中最小，获取锁
				return;
			} else {
				// 获取当前节点的前一个节点
				preNode = rootNode + "/" + children.get(thisNodeIndex - 1);
				// 在preNode上注册监视器，如果preNode被删除，则调用监视器的process方法
				zk.getData(preNode, true, new Stat());
				// 进程等待
				preNodeLatch.await();
				return;
			}
		}
	}

	/**
	 * 删除锁
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void zkUnLock() throws InterruptedException, KeeperException {
		zk.delete(this.currentNode, -1);
	}

}
