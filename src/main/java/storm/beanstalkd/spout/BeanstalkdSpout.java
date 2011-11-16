package storm.beanstalkd.spout;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import com.surftools.BeanstalkClient.Client;
import com.surftools.BeanstalkClient.Job;
import com.surftools.BeanstalkClientImpl.ClientImpl;

import backtype.storm.spout.Scheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;

/**
 * storm spout for storm
 * 
 * @author haitao.yao
 * 
 */
public class BeanstalkdSpout implements IRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5272136039973553531L;

	private final Scheme serialisationScheme;

	private static final int DEFAULT_RESERVE_TIMEOUT = 100;

	private transient Client beanstalkdClient;

	private SpoutOutputCollector collector;

	private final BeanstalkdConfig config;

	public BeanstalkdSpout(BeanstalkdConfig config, Scheme serialisationScheme) {
		this.serialisationScheme = serialisationScheme;
		this.config = config;
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf,
			TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.beanstalkdClient = new ClientImpl(this.config.host,
				this.config.port);
		this.beanstalkdClient.watch(this.config.topic);
	}

	@Override
	public void close() {
		this.beanstalkdClient.close();
	}

	@Override
	public void nextTuple() {
		Job job = this.beanstalkdClient.reserve(DEFAULT_RESERVE_TIMEOUT);
		if (job == null) {
			return;
		}
		List<Object> data = this.serialisationScheme.deserialize(job.getData());
		long jobId = job.getJobId();
		this.collector.emit(data, jobId);
	}

	@Override
	public void ack(Object msgId) {
		if (msgId instanceof Long) {
			long jobId = (Long) msgId;
			this.beanstalkdClient.delete(jobId);
		}
	}

	@Override
	public void fail(Object msgId) {
		if (msgId instanceof Long) {
			long jobId = (Long) msgId;
			this.beanstalkdClient.release(jobId, this.config.jobPriority,
					this.config.delaySeconds);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(this.serialisationScheme.getOutputFields());
	}

	@Override
	public boolean isDistributed() {
		return config.distributed;
	}

	/**
	 * configuration for beanstalkd
	 * 
	 * @author haitao.yao
	 * 
	 */
	public static class BeanstalkdConfig implements Serializable {

		private static final long serialVersionUID = 8061388625541783064L;

		private final String host;

		private final int port;

		private final String topic;

		private int jobPriority = 10;

		private int delaySeconds = 0;

		private boolean distributed = true;

		public BeanstalkdConfig(String host, int port, String topic) {
			super();
			this.host = host;
			this.port = port;
			this.topic = topic;
		}

		public String getHost() {
			return host;
		}

		public int getPort() {
			return port;
		}

		public String getTopic() {
			return topic;
		}

		public int getJobPriority() {
			return jobPriority;
		}

		public void setJobPriority(int jobPriority) {
			this.jobPriority = jobPriority;
		}

		public int getDelaySeconds() {
			return delaySeconds;
		}

		public void setDelaySeconds(int delaySeconds) {
			this.delaySeconds = delaySeconds;
		}

		public boolean isDistributed() {
			return distributed;
		}

		public void setDistributed(boolean distributed) {
			this.distributed = distributed;
		}

	}

}
