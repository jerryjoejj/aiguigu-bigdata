package lma.curatortest;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class CuratorLockTest {

	private final String connectString = "hadoop001:2181,hadoop002:2181,hadoop003:2181";
	private final int sessionTimeout = 2000;
	private final int connectionTimeout = 3000;
	private final String rootNode = "/lock";

	public static void main(String[] args) {
		new CuratorLockTest().test();
	}


	private void test() {
		InterProcessMutex lock1 = new InterProcessMutex(getCuratorFramework(), rootNode);
		InterProcessMutex lock2 = new InterProcessMutex(getCuratorFramework(), rootNode);


		new Thread(() -> {
			try {
				lock1.acquire();
				System.out.println("线程1获取锁");
				lock1.acquire();
				System.out.println("线程1重新获取锁");
				Thread.sleep(5 * 1000);
				lock1.release();
				System.out.println("线程1释放锁");
				lock1.release();
				System.out.println("线程1再次释放锁");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}).start();

		new Thread(() -> {
			try {
				lock2.acquire();
				System.out.println("线程2获取锁");
				lock2.acquire();
				System.out.println("线程2重新获取锁");
				Thread.sleep(5 * 1000);
				lock2.release();
				System.out.println("线程2释放锁");
				lock2.release();
				System.out.println("线程2再次释放锁");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}).start();
	}

	public CuratorFramework getCuratorFramework() {
		RetryPolicy policy = new ExponentialBackoffRetry(3000, 3);

		CuratorFramework curatorClient = CuratorFrameworkFactory
				.builder()
				.connectString(connectString) // zookeeper集群地址
				.connectionTimeoutMs(connectionTimeout) // 连接超时事件
				.sessionTimeoutMs(sessionTimeout) // 会话超时事件
				.retryPolicy(policy)  // 重试策略
				.build();

		curatorClient.start();
		System.out.println("zookeeper 初始化完成....");
		return curatorClient;
	}

}
