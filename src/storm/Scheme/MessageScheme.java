package storm.Scheme;

import java.io.UnsupportedEncodingException;
import java.util.List;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MessageScheme implements Scheme {

	/**
	 * @Fields serialVersionUID : TODO(用一句话描述这个变量表示什么)
	 */
	private static final long serialVersionUID = 3029557435066271451L;

	@Override
	public List<Object> deserialize(byte[] bytes) {
		try {
			String msg = new String(bytes, "UTF-8");
			return new Values(msg);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Fields getOutputFields() {
		return new Fields("order");
	}

}
