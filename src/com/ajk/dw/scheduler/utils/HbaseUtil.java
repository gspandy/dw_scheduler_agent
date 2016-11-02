package com.ajk.dw.scheduler.utils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ajk.dw.scheduler.common.SchedulerConfig;
import com.ajk.dw.scheduler.common.SchedulerConfigFactory;

public class HbaseUtil {

	private static final Logger LOG=LoggerFactory.getLogger(HbaseUtil.class);
	private static SchedulerConfig config = SchedulerConfigFactory.getDroneConf();
	private static String HbaseRestApi = config.getString(SchedulerConfig.HBASE_REST_URL);
	//private static String HbaseRestApi = "http://10.20.8.38:20550";
	/**
	 * 提交数据到Hbase
	 * @param tableName
	 * @param columnName
	 * @param data
	 * @return
	 */
    public static String hbasePutRestApi(String tableName,String rowKey,String columnName,String data){
    	HttpPost post = new HttpPost(HbaseRestApi+"/"+tableName+"/"+rowKey
    			+"/"+columnName);
        List <BasicNameValuePair> nvps = new ArrayList <BasicNameValuePair>();
        nvps.add(new BasicNameValuePair("data", data));
        try {
			post.setEntity(new UrlEncodedFormEntity(nvps, HTTP.UTF_8));
			post.setHeader("Content-Type", "application/octet-stream");
		} catch (UnsupportedEncodingException e1) {
			e1.printStackTrace();
			LOG.error("hbasePutRestApi执行ID" + rowKey + ",put Hbase失败,errror message:",e1.getStackTrace());
		}

        HttpClient httpclient = new DefaultHttpClient();

        try {
            ResponseHandler<String> responseHandler = new BasicResponseHandler();
            return httpclient.execute(post, responseHandler);
        } catch (ClientProtocolException e) {
            e.printStackTrace();
            LOG.error("hbasePutRestApi执行ID" + rowKey + ",put Hbase失败,errror message:"+e.getStackTrace());
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("hbasePutRestApi执行ID" + rowKey + ",put Hbase失败,errror message:"+e.getStackTrace());
        }
        return null;
    }

    public static String hbaseGetRestApi(String tableName,String rowKey,String columnName){
    	HttpGet httpGet = new HttpGet(HbaseRestApi+"/"+tableName+"/"+rowKey+"/"+columnName);
    	httpGet.setHeader("Content-Type", "application/octet-stream");
    	httpGet.setHeader("Accept", "application/json");
        HttpClient httpclient = new DefaultHttpClient();
        try {
            ResponseHandler<String> responseHandler = new BasicResponseHandler();
            return httpclient.execute(httpGet, responseHandler);
        } catch (ClientProtocolException e) {
            e.printStackTrace();
            LOG.error("hbaseGetRestApi执行ID" + rowKey + ",put Hbase失败,errror message:"+e.getStackTrace());

        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("hbaseGetRestApi执行ID" + rowKey + ",put Hbase失败,errror message:"+e.getStackTrace());

        }
        return null;
    }

    public static void  main(String[] args){
    	String jsonLog = HbaseUtil.hbaseGetRestApi("dw_scheduler_job_log", "277054", "runlog");
    	/*JSONObject aa = JSONObject.
    	String decoded = Base64.base64Decode();*/
    	System.out.println(jsonLog);
    }

}
