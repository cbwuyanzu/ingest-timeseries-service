package com.ge.predix.solsvc.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import javax.annotation.PostConstruct;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import org.springframework.scheduling.annotation.Scheduled;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.http.Header;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.GzipDecompressingEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ge.predix.solsvc.api.IngestDataAPI;
import com.ge.predix.solsvc.restclient.impl.CxfAwareRestClient;
import com.ge.predix.solsvc.spi.IServiceManagerService;
import com.ge.predix.solsvc.timeseries.bootstrap.config.TimeseriesRestConfig;
import com.ge.predix.solsvc.timeseries.bootstrap.config.TimeseriesWSConfig;
import com.ge.predix.solsvc.timeseries.bootstrap.factories.TimeseriesFactory;
import com.ge.predix.solsvc.timeseries.bootstrap.websocket.client.TimeseriesWebsocketClient;
import com.ge.predix.timeseries.entity.datapoints.ingestionrequest.Body;
import com.ge.predix.timeseries.entity.datapoints.ingestionrequest.DatapointsIngestion;
import com.ge.predix.timeseries.entity.datapoints.queryrequest.DatapointsQuery;
import com.ge.predix.timeseries.entity.datapoints.queryrequest.latest.DatapointsLatestQuery;
import com.ge.predix.timeseries.entity.datapoints.queryresponse.DatapointsResponse;

/**
 * 
 * @author predix -
 */
@Component
public class IngestDataImpl implements IngestDataAPI {

	@Autowired
	private IServiceManagerService serviceManagerService;

	@Autowired
	private TimeseriesRestConfig timeseriesRestConfig;

	@Autowired
	private CxfAwareRestClient restClient;

	@Autowired
	private TimeseriesWSConfig tsInjectionWSConfig;

	@Autowired
	private TimeseriesWebsocketClient timeseriesWebsocketClient;

	@Autowired
	private TimeseriesFactory timeseriesFactory;

	private static Logger log = LoggerFactory.getLogger(IngestDataImpl.class);

	/**
	 * -
	 */
	public IngestDataImpl() {
		super();
	}

	/**
	 * -
	 */
	@PostConstruct
	public void init() {
		this.serviceManagerService.createRestWebService(this, null);
		createMetrics();
		createGEMetrics();
	}

	@Scheduled(cron = "0 0 * * * ?")
	public void dataIngest() {
		createMetrics();
		createGEMetrics();
	}

	@Override
	public Response greetings() {
		return handleResult("Greetings from CXF Bean Rest Service " + new Date()); //$NON-NLS-1$
	}

	/**
	 * 
	 * @param s
	 *            -
	 * @return
	 */
	private int getInteger(String s) {
		int inValue = 25;
		try {
			inValue = Integer.parseInt(s);

		} catch (NumberFormatException ex) {
			// s is not an integer
		}
		return inValue;
	}

	@SuppressWarnings({ "unqualified-field-access", "nls" })
	private List<Header> generateHeaders() {
		List<Header> headers = this.restClient.getSecureTokenForClientId();
		this.restClient.addZoneToHeaders(headers, this.timeseriesRestConfig.getZoneId());
		return headers;
	}

	private DatapointsLatestQuery buildLatestDatapointsQueryRequest(String id) {
		DatapointsLatestQuery datapointsLatestQuery = new DatapointsLatestQuery();

		com.ge.predix.timeseries.entity.datapoints.queryrequest.latest.Tag tag = new com.ge.predix.timeseries.entity.datapoints.queryrequest.latest.Tag();
		tag.setName(id);
		List<com.ge.predix.timeseries.entity.datapoints.queryrequest.latest.Tag> tags = new ArrayList<com.ge.predix.timeseries.entity.datapoints.queryrequest.latest.Tag>();
		tags.add(tag);
		datapointsLatestQuery.setTags(tags);
		return datapointsLatestQuery;
	}

	/**
	 * 
	 * @param id
	 * @param startDuration
	 * @param tagorder
	 * @return
	 */
	private DatapointsQuery buildDatapointsQueryRequest(String id, String startDuration, int taglimit,
			String tagorder) {
		DatapointsQuery datapointsQuery = new DatapointsQuery();
		List<com.ge.predix.timeseries.entity.datapoints.queryrequest.Tag> tags = new ArrayList<com.ge.predix.timeseries.entity.datapoints.queryrequest.Tag>();
		datapointsQuery.setStart(startDuration);
		// datapointsQuery.setStart("1y-ago"); //$NON-NLS-1$
		String[] tagArray = id.split(","); //$NON-NLS-1$
		List<String> entryTags = Arrays.asList(tagArray);

		for (String entryTag : entryTags) {
			com.ge.predix.timeseries.entity.datapoints.queryrequest.Tag tag = new com.ge.predix.timeseries.entity.datapoints.queryrequest.Tag();
			tag.setName(entryTag);
			tag.setLimit(taglimit);
			tag.setOrder(tagorder);
			tags.add(tag);
		}
		datapointsQuery.setTags(tags);
		return datapointsQuery;
	}

	@SuppressWarnings({ "nls", "unchecked" })
	private void createMetrics() {
		String newInfo = "";
		for (int connectcount = 0; connectcount < 5; connectcount++) {
			newInfo = getJsonContent("http://www.mypm25.cn/controlPoint?cityName=上海");
			if (!newInfo.equals("")) {
				for (int i = 0; i < 10; i++) {
					Pm25Sensor pm25 = getPm25(newInfo, i);
					DatapointsIngestion dpIngestion = new DatapointsIngestion();
					dpIngestion.setMessageId(String.valueOf(System.currentTimeMillis()));
					Body body = new Body();
					body.setName(pm25.getPointID()); // $NON-NLS-1$
					List<Object> datapoint1 = new ArrayList<Object>();
					datapoint1.add(System.currentTimeMillis());
					datapoint1.add(pm25.getMeasure());
					datapoint1.add(3); // quality
					List<Object> datapoints = new ArrayList<Object>();
					datapoints.add(datapoint1);
					body.setDatapoints(datapoints);
					com.ge.dsp.pm.ext.entity.util.map.Map map = new com.ge.dsp.pm.ext.entity.util.map.Map();
					map.put("site", pm25.getCity() + pm25.getPointName()); // $NON-NLS-2$
					map.put("lat", pm25.getyValue());
					map.put("lng", pm25.getxValue());
					body.setAttributes(map);
					List<Body> bodies = new ArrayList<Body>();
					bodies.add(body);
					dpIngestion.setBody(bodies);
					this.timeseriesFactory.create(dpIngestion);
				}
				break;
			}
		}
	}

	@SuppressWarnings({ "nls", "unchecked" })
	private void createGEMetrics() {
		String newInfoIn = "";
		String newInfoOut = "";
		for (int connectcount = 0; connectcount < 5; connectcount++) {
			newInfoOut = getJsonContent("http://www.mypm25.cn/countOne.action?di.devid=61728");
			newInfoIn = getJsonContent("http://www.mypm25.cn/countOne.action?di.devid=61726");
			if (!newInfoOut.equals("")) {
				Pm25Sensor pm25 = getGEOutPm25(newInfoOut);
				DatapointsIngestion dpIngestion = new DatapointsIngestion();
				dpIngestion.setMessageId(String.valueOf(System.currentTimeMillis()));
				Body body = new Body();
				body.setName(pm25.getPointID()); // $NON-NLS-1$
				List<Object> datapoint1 = new ArrayList<Object>();
				datapoint1.add(System.currentTimeMillis());
				datapoint1.add(pm25.getMeasure());
				datapoint1.add(3); // quality
				List<Object> datapoints = new ArrayList<Object>();
				datapoints.add(datapoint1);
				body.setDatapoints(datapoints);
				com.ge.dsp.pm.ext.entity.util.map.Map map = new com.ge.dsp.pm.ext.entity.util.map.Map();
				map.put("site", pm25.getCity() + pm25.getPointName()); // $NON-NLS-2$
				map.put("lat", pm25.getyValue()+"");
				map.put("lng", pm25.getxValue()+"");
				body.setAttributes(map);
				List<Body> bodies = new ArrayList<Body>();
				bodies.add(body);
				dpIngestion.setBody(bodies);
				this.timeseriesFactory.create(dpIngestion);
			}
			if (!newInfoIn.equals("")) {
				Pm25Sensor pm25 = getGEInPm25(newInfoIn);
				DatapointsIngestion dpIngestion = new DatapointsIngestion();
				dpIngestion.setMessageId(String.valueOf(System.currentTimeMillis()));
				Body body = new Body();
				body.setName(pm25.getPointID()); // $NON-NLS-1$
				List<Object> datapoint1 = new ArrayList<Object>();
				datapoint1.add(System.currentTimeMillis());
				datapoint1.add(pm25.getMeasure());
				datapoint1.add(3); // quality
				List<Object> datapoints = new ArrayList<Object>();
				datapoints.add(datapoint1);
				body.setDatapoints(datapoints);
				com.ge.dsp.pm.ext.entity.util.map.Map map = new com.ge.dsp.pm.ext.entity.util.map.Map();
				map.put("site", pm25.getCity() + pm25.getPointName()); // $NON-NLS-2$
				map.put("lat", pm25.getyValue());
				map.put("lng", pm25.getxValue());
				body.setAttributes(map);
				List<Body> bodies = new ArrayList<Body>();
				bodies.add(body);
				dpIngestion.setBody(bodies);
				this.timeseriesFactory.create(dpIngestion);
			}
			break;
		}
	}

	@SuppressWarnings("javadoc")
	protected Response handleResult(Object entity) {
		ResponseBuilder responseBuilder = Response.status(Status.OK);
		responseBuilder.type(MediaType.APPLICATION_JSON);
		responseBuilder.entity(entity);
		return responseBuilder.build();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ge.predix.solsvc.api.WindDataAPI#getWindDataTags()
	 */

	/**
	 * 
	 * @param entity
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings("nls")
	private String processHttpResponseEntity(org.apache.http.HttpEntity entity) throws IOException {
		if (entity == null)
			return null;
		if (entity instanceof GzipDecompressingEntity) {
			return IOUtils.toString(((GzipDecompressingEntity) entity).getContent(), "UTF-8");
		}
		return EntityUtils.toString(entity);
	}

	public String getJsonContent(String urlStr) {
		try {// 获取HttpURLConnection连接对象
			URL url = new URL(urlStr);
			HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
			// 设置连接属性
			httpConn.setConnectTimeout(5000);
			httpConn.setDoInput(true);
			httpConn.setRequestMethod("GET");
			httpConn.connect();
			BufferedReader reader = new BufferedReader(new InputStreamReader(httpConn.getInputStream()));
			String lines;
			StringBuilder sb = new StringBuilder();
			while ((lines = reader.readLine()) != null) {
				sb.append(lines);
			}
			reader.close();
			// 断开连接
			httpConn.disconnect();
			return sb.toString();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "";
	}

	public Pm25Sensor getPm25(String jsonStr, int i) {
		Pm25Sensor pm25 = new Pm25Sensor();
		try {// 将json字符串转换为json对象
			JSONArray jsonArr = new JSONArray(jsonStr);
			JSONObject obj = (JSONObject) jsonArr.get(i);
			pm25.setCity(obj.getString("cityName"));
			pm25.setMeasure(obj.getInt("pm2_5"));
			pm25.setPointID(obj.getString("pointId"));
			pm25.setPointName(obj.getString("pointName"));
			pm25.setUpdateTime(obj.getString("updateTime"));
			pm25.setxValue(obj.getString("xValue"));
			pm25.setyValue(obj.getString("yValue"));
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return pm25;
	}

	public Pm25Sensor getGEInPm25(String jsonStr) {
		Pm25Sensor pm25 = new Pm25Sensor();
		try {// 将json字符串转换为json对象
			JSONObject jsonobj = new JSONObject(jsonStr);
			JSONArray jsonArr = jsonobj.getJSONArray("jsonlist");
			JSONObject obj = (JSONObject) jsonArr.get(0);
			pm25.setCity("上海");
			pm25.setMeasure(obj.getInt("pmvalue"));
			pm25.setPointID(jsonobj.getJSONObject("di").getString("devid"));
			pm25.setPointName("通用电气（室内）");
			pm25.setUpdateTime(obj.getString("time"));
			pm25.setxValue("121.590100");
			pm25.setyValue("31.197586");
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return pm25;
	}

	public Pm25Sensor getGEOutPm25(String jsonStr) {
		Pm25Sensor pm25 = new Pm25Sensor();
		try {// 将json字符串转换为json对象
			JSONObject jsonobj = new JSONObject(jsonStr);
			JSONArray jsonArr = jsonobj.getJSONArray("jsonlist");
			JSONObject obj = (JSONObject) jsonArr.get(0);
			pm25.setCity("上海");
			pm25.setMeasure(obj.getInt("pmvalue"));
			pm25.setPointID(jsonobj.getJSONObject("di").getString("devid"));
			pm25.setPointName("通用电气（室外）");
			pm25.setUpdateTime(obj.getString("time"));
			pm25.setxValue("121.589362");
			pm25.setyValue("31.198181");
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return pm25;
	}

	@Override
	public Response postDataPoints(String measure, String tag) {
		{
			DatapointsIngestion dpIngestion = new DatapointsIngestion();
			dpIngestion.setMessageId(String.valueOf(System.currentTimeMillis()));

			Body body = new Body();
			body.setName(tag); // $NON-NLS-1$
			List<Object> datapoint1 = new ArrayList<Object>();
			datapoint1.add(System.currentTimeMillis());
			datapoint1.add(measure);
			datapoint1.add(3); // quality

			List<Object> datapoints = new ArrayList<Object>();
			datapoints.add(datapoint1);
			body.setDatapoints(datapoints);
			com.ge.dsp.pm.ext.entity.util.map.Map map = new com.ge.dsp.pm.ext.entity.util.map.Map();
			map.put("site", "heihei"); // $NON-NLS-2$
			// map.put("lat", pm25.getyValue());
			// map.put("lng", pm25.getxValue());
			body.setAttributes(map);

			List<Body> bodies = new ArrayList<Body>();
			bodies.add(body);

			dpIngestion.setBody(bodies);
			this.timeseriesFactory.create(dpIngestion);
		}
		return handleResult("You've successfully ingest a datapoint " + new Date());
	}

}
