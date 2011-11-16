package storm.beanstalkd.spout;

import java.util.HashMap;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import storm.beanstalkd.spout.BeanstalkdSpout.BeanstalkdConfig;
import backtype.storm.spout.RawScheme;
import backtype.storm.spout.Scheme;

import com.surftools.BeanstalkClient.Client;
import com.surftools.BeanstalkClientImpl.ClientImpl;

public class BeanstalkdSpoutTest {

	private BeanstalkdConfig config = new BeanstalkdConfig("test", 11300, TOPIC);

	private static final String TOPIC = "test";

	private BeanstalkdSpout spout = null;

	private final Scheme scheme = new RawScheme();

	@SuppressWarnings("rawtypes")
	@Before
	public void setUp() {
		this.spout = new BeanstalkdSpout(config, this.scheme);
		this.spout.open(new HashMap(), null, null);
	}

	@Test
	@Ignore
	public void testNextTuple() {
		this.spout.nextTuple();
	}

	@Test
	@Ignore
	public void sendTestData() {
		Client client = new ClientImpl(this.config.getHost(),
				this.config.getPort());
		client.watch(this.config.getTopic());
		client.put(1, 1, 0, "testtesttest".getBytes());
	}

}
