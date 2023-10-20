package com.takeda.etl;

import java.math.BigDecimal; 
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager; 
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.takeda.rds.cb4.model.PlatoContainer;
import com.takeda.rds.cb4.service.PlatoBaseService;
import com.takeda.rds.dsis.idl.ProcessSampleData;
import com.takeda.rds.dsis.util.TimeWatch;
import com.takeda.rds.grdb.service.ConfigService;
import com.takeda.rds.mosaic.model.MosaicInvRegCompound;
import com.takeda.rds.mosaic.model.MosaicInvSolvent;
import com.takeda.rds.mosaic.model.SampleHolderData;
import com.takeda.rds.mosaic.service.MosaicBaseService;
import com.takeda.rds.mosaic.service.MosaicCompoundService;
import com.takeda.rds.mosaic.service.MosaicInvSampleHolderService;
import com.takeda.rds.mosaic.service.MosaicInvSolventService;
import com.takeda.rds.tbos.util.EmailUtils;

public class ETLJob extends ProcessSampleData { 

	//public static Logger logger = LogManager.getLogger(ETLJob.class);
	public static Logger logger = LogManager.getLogger(ETLJob.class);
	protected static TimeWatch watch = TimeWatch.start();
	
	protected Connection connection = null;
	protected Connection mosaicConnection = null;
	protected String host = null;
	protected String name = null;
	protected String userName = null;
	protected String password = null;
	protected String databaseType = null;
	protected String port = null;
	private String moisaicUserName;
	private String mosaicPassword;
	private String mosaicName;
	private String APIUserName;
	private String APIPassword;
	private String authHeader;
	private String mosaicAPIUrl;
		
	EmailUtils emailUtils = null;
	
	private int listSize;

	public static final String FETCH_MODIFIED_SAMPLE_DATA = "SELECT "
			+ " ct.mosaic_type as dstCCType , ctypedest.mosaic_type as destType,"
			+ "	ctype.mosaic_type as sourceType , ctsub.mosaic_type as sourceCCType, "			
			+ "OPERATION_SID ,ACTION_DATE ,SUBJECT_CONTAINER_SID ,SUBJECT_CONTAINER_ID , "
			+ " SUBJECT_CONTAINER_NETWT ,SUBJECT_CONTAINER_NETWT_UNITS ,SUBJECT_CONTAINER_CONC , "
			+ " SUBJECT_CONTAINER_CONC_UNITS ,OBJECT_CONTAINER_SID ,OBJECT_CONTAINER_ID , "
			+ " OBJECT_CONTAINER_CONC ,OBJECT_CONTAINER_CONC_UNITS ,OPERATION_TYPE ,TRANSFER_AMOUNT , "
			+ " TRANSFER_UNITS ,SOLVENT_AMOUNT ,SOLVENT_UNITS ,ACTION_TEXT ,ACTION_LOCATION ,  "
			+ " cntr.tare_wt as source_tarewt, cntr.mass_uom as source_uom,  "
			+ " cntrdest.tare_wt as dest_tarewt, cntrdest.mass_uom as dest_uom , que.subject_collection_id , que.set_name , "
			+ " que.compound_id , subject_container_volume , que.solvent  "
			+ "  from plato.v_mosaic_operation_queue que  "
			+ "  left join plato.container cntr on que.SUBJECT_CONTAINER_SID = cntr.container_sid  "
			+ "  left join plato.lu_container_type ctype on cntr.container_type_sid = ctype.container_type_sid "
			+ "  LEFT join plato.container_collection ccsub on cntr.container_collection_sid = ccsub.container_collection_sid "
			+ "  left join plato.lu_cont_coll_type ctsub on ccsub.cont_coll_type_sid = ctsub.cont_coll_type_sid "
			+ "   left join plato.container cntrdest on que.OBJECT_CONTAINER_SID = cntrdest.container_sid  "
			+ "  left join plato.lu_container_type ctypedest on cntrdest.container_type_sid = ctypedest.container_type_sid "
			+ "  LEFT join plato.container_collection cc on cntrdest.container_collection_sid = cc.container_collection_sid "
 			+ "  left join plato.lu_cont_coll_type ct on cc.cont_coll_type_sid = ct.cont_coll_type_sid order by operation_sid ASC";

	public static final String FETCH_LABWAREITEMID_FOR_CONTAINER_BARCODE = "select il.LABWAREITEMID as LABWAREITEMID, sh.SUBSTANCEID as SUBSTANCEID from inv_labware_item il "
			+ " LEFT JOIN INV_SAMPLE_HOLDER sh on il.LABWAREITEMID=sh.LABWAREITEMID " + " where  il.labwarebarcode=?";
	
	Connection platoConnection = null;
	PlatoBaseService platoBaseService = new PlatoBaseService();
	MosaicBaseService mosaicBaseService = new MosaicBaseService();
	public Connection initMosaicConnection() { 

		Connection mosaicConnection = null;
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			// logger.debug(".........MOSAIC..............");
			String url = "jdbc:oracle:thin:@" + this.mosaicName;
			mosaicConnection = DriverManager.getConnection(url, this.moisaicUserName, this.mosaicPassword);
			logger.debug("Mosaic Database Connection Object: " + mosaicConnection);

		} catch (ClassNotFoundException e) {
			logger.error("Where is your Oracle JDBC Driver?");
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return mosaicConnection;
	}

	public Connection getMosaicConnection() {
		if (this.mosaicConnection == null)
			this.mosaicConnection = initMosaicConnection();
		return this.mosaicConnection;
	}

	public void closeMosaicConnection() {
		try {
			getMosaicConnection().close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public ETLJob() {
		
		Properties prop = ConfigService.readProperties();
		
		//listSize = Integer.parseInt( prop.getProperty("list.size") );
		listSize=2000;
		if (prop.getProperty("titian.mosaic.inventory.api.url") != null)
			this.mosaicAPIUrl = prop.getProperty("titian.mosaic.inventory.api.url");
		if (prop.getProperty("titian.mosaic.api.user") != null)
			this.APIUserName = prop.getProperty("titian.mosaic.api.user");
		if (prop.getProperty("titian.mosaic.api.password") != null)
			this.APIPassword = prop.getProperty("titian.mosaic.api.password");
		
		String auth = APIUserName + ":" + APIPassword;
		logger.debug("auth ->" + auth);
		byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.ISO_8859_1));
		this.authHeader = "Basic " + new String(encodedAuth);

	
		try {
			for (int i=0; i<3; i++) {
				
				if (platoConnection==null || platoConnection.isClosed()) {
					platoConnection = platoBaseService.getCB4PlatoConnection();
				}
				
				if (mosaicConnection==null || mosaicConnection.isClosed()) {
					mosaicConnection = mosaicBaseService.getMosaicConnection();
				}
			}
		} catch (SQLException e) {
			logger.debug(e.getMessage());
		}		
		
		
	}

	private String getAPIResponse(String apiUrl, String htmMethod, StringEntity params) throws Exception {

		String jsonResult = "[]";
		HttpResponse response = null;
		HttpClient client;

		try {

			logger.debug("api url ..." + apiUrl);
			if (htmMethod.equalsIgnoreCase("GET")) {
				//logger.debug("Inside GET");
				HttpGet request = new HttpGet(apiUrl);
				request.setHeader(HttpHeaders.AUTHORIZATION, this.authHeader);
				request.setHeader(HttpHeaders.CONTENT_TYPE, PlatoMosaicConstants.CONTENT_TYPE);
				client = HttpClientBuilder.create().build();
				response = client.execute(request);
			}
			if (htmMethod.equalsIgnoreCase("POST")) {
				//logger.debug("Inside POST");
				HttpPost request = new HttpPost(apiUrl);
				request.setHeader(HttpHeaders.AUTHORIZATION, this.authHeader);
				request.setHeader(HttpHeaders.CONTENT_TYPE, PlatoMosaicConstants.CONTENT_TYPE);
				request.setHeader(HttpHeaders.ACCEPT, PlatoMosaicConstants.CONTENT_TYPE);
				request.setEntity(params);
				client = HttpClientBuilder.create().build();
				response = client.execute(request);
			}
			if (htmMethod.equalsIgnoreCase("PATCH")) {
				logger.debug("Inside PATCH..." + apiUrl);
				HttpPatch request = new HttpPatch(apiUrl);
				request.setHeader(HttpHeaders.AUTHORIZATION, this.authHeader);
				request.setHeader(HttpHeaders.CONTENT_TYPE, PlatoMosaicConstants.CONTENT_TYPE);
				request.addHeader(HttpHeaders.CONTENT_TYPE, PlatoMosaicConstants.CONTENT_TYPE);
				request.setEntity(params);
				client = HttpClientBuilder.create().build();
				response = client.execute(request);
			}

			logger.debug("response value :" + response);
			if (!response.toString().contains("No Content"))
				jsonResult = EntityUtils.toString(response.getEntity());
		} catch (Exception e) {
			 logger.debug("Exception message...." +e.getMessage());
			//e.printStackTrace();
		}
		return jsonResult;
	}

	/*
	 * ETL
	 * jobs.........................................................................
	 * ..............................
	 */

	private synchronized String tare(cb4DTO cb4dto) throws Exception {

		logger.debug("TARE Weight starts here....................");
		PreparedStatement prepStmt = null;
		ResultSet rs = null;
		String sourceLabwareItemId = "";
		String sourceSubstanceId = "";
		String jsonResult = "";
		String sourceXPosition = "";
		String sourceYPosition = "";
		String url = this.mosaicAPIUrl;  //: https://takeda.mosaic-cloud.com/api/Inventory //"https://takeda.mosaic-cloud.com/api/Inventory";
		
		try {
			prepStmt = mosaicConnection.prepareStatement(FETCH_LABWAREITEMID_FOR_CONTAINER_BARCODE);
			prepStmt.setString(1, cb4dto.getSubjectContainerId());
			rs = prepStmt.executeQuery();
			if (rs.next()) {
				sourceLabwareItemId = rs.getString("LABWAREITEMID");
				sourceSubstanceId = rs.getString("SUBSTANCEID");
			}
			logger.debug("TARE source labwareItemId..." + sourceLabwareItemId);
			prepStmt.close();
			rs.close();
			
			//jsonResult = this.getSampleHolders("testB0243190", "testType");
			jsonResult = this.getSampleHolders(cb4dto.getSubjectContainerId().split(":")[0], cb4dto.getMosaicType());
					
			logger.debug("TARE-- sample holder details are available: " + jsonResult);
			jsonResult = jsonResult.substring(1, jsonResult.length() - 1);
			JSONObject obj = new JSONObject(jsonResult.toString());
			JSONArray sampleHolders = obj.getJSONArray("sampleHolders");
			JSONObject sampleHolderIdentifier = new JSONObject(
					sampleHolders.getJSONObject(0).get("sampleHolderIdentifier").toString());
			JSONObject position = new JSONObject(sampleHolderIdentifier.get("position").toString());
			sourceXPosition = position.get("x").toString();
			sourceYPosition = position.get("y").toString();
			// logger.debug("sourceXPosition = " + sourceXPosition + " sourceYPosition
			// = " + sourceYPosition);
			if (sourceLabwareItemId == null || sourceLabwareItemId.equals("")) {
				sourceLabwareItemId = sampleHolderIdentifier.get("labwareItemId").toString();
			}
			
			url = url + "/LabwareItems/" + sourceLabwareItemId + "";

			logger.debug("TARE api..." + url);
			logger.debug("TARE Weight to Transfer : " + cb4dto.getSourceTareWt());

			StringEntity params = new StringEntity(
					"{" + "  \"tareWeight\": {" + "    \"value\": " + cb4dto.getSourceTareWt() + ",\"displayExponent\": -3,\"resolution\": -2 " + "  }" + "} ");
			jsonResult = getAPIResponse(url, "PATCH", params);
			logger.debug("TARE output done ");
			logger.debug("TARE ..........jsonResult=" + jsonResult);
			updateQueue(jsonResult, cb4dto);
			return jsonResult;
		} catch (Exception e) {
			if (rs != null && !rs.isClosed())
				rs.close();
			if (prepStmt != null && !prepStmt.isClosed())
				prepStmt.close();
			//throw e;
			logger.debug("------------------------tracking the error.............." + e.getMessage());
			updateQueue(e.getMessage(), cb4dto);
			return e.getMessage();
		} finally {

		}
	}

	// Submit

	private synchronized String submit(cb4DTO cb4dto) throws Exception {

		logger.debug("SUBMIT starts here...................." + 1);
		String jsonResult = ""; 
		boolean checkForCompound=false;
		try {
			
			checkForCompound= this.checkIfCompoundExists(cb4dto);
			if (checkForCompound ) {
				Float isDry = cb4dto.getSubjectContainerNetwt();
				String vol = cb4dto.getSubjectContainerVolume();
				Float volConc = cb4dto.getSubjectContainerConc();
				logger.debug("isDry " + isDry);
				if (vol!=null && volConc > 0) {
					jsonResult = this.setSolutionSample(cb4dto);
				} else {
					jsonResult = this.setNeatSample(cb4dto);
				}
				updateQueue(jsonResult, cb4dto);
			} else {
				updateQueue("Compound does not exists for this barcode : " + cb4dto.getSubjectContainerId(), cb4dto);
			}
			
			return jsonResult;
		} catch (Exception e) {
			logger.debug("------------------------tracking the error.............." + e.getMessage());
			updateQueue(e.getMessage(), cb4dto);
			return e.getMessage();
		} finally {

		}
	}
	// final weigh

	private synchronized String finalWeigh(cb4DTO cb4dto) throws Exception {

		logger.debug("Final Weigh starts here....................");
		PreparedStatement prepStmt = null;
		ResultSet rs = null;
		String sourceLabwareItemId = "";
		String jsonResult = "";
		String sourceXPosition = "";
		String sourceYPosition = "";
		String url = this.mosaicAPIUrl; //"https://takeda.mosaic-cloud.com/api/Inventory";
		try {
			prepStmt = mosaicConnection.prepareStatement(FETCH_LABWAREITEMID_FOR_CONTAINER_BARCODE);
			prepStmt.setString(1, cb4dto.getSubjectContainerId());
			rs = prepStmt.executeQuery();
			if (rs.next())
				sourceLabwareItemId = rs.getString("LABWAREITEMID");

			logger.debug("Final Weigh source labwareItemId..." + sourceLabwareItemId);
			prepStmt.close();
			rs.close();
			jsonResult = this.getSampleHolders(cb4dto.getSubjectContainerId().split(":")[0], cb4dto.getMosaicType());
			logger.debug("final weigh-- sample holder details are available: " + jsonResult);
			jsonResult = jsonResult.substring(1, jsonResult.length() - 1);
			JSONObject obj = new JSONObject(jsonResult.toString());
			JSONArray sampleHolders = obj.getJSONArray("sampleHolders");
			JSONObject sampleHolderIdentifier = new JSONObject(
					sampleHolders.getJSONObject(0).get("sampleHolderIdentifier").toString());
			JSONObject position = new JSONObject(sampleHolderIdentifier.get("position").toString());
			sourceXPosition = position.get("x").toString();
			sourceYPosition = position.get("y").toString();
			// logger.debug("sourceXPosition = " + sourceXPosition + " sourceYPosition
			// = " + sourceYPosition);
			
			if (sourceLabwareItemId == null || sourceLabwareItemId.equals("")) {
				sourceLabwareItemId = sampleHolderIdentifier.get("labwareItemId").toString();
		}
			
			url = url + "/SampleHolders/" + sourceLabwareItemId + "@(" + sourceXPosition + "," + sourceYPosition + ")";
			logger.debug("final-weigh api..." + url);
			logger.debug("Gross Weight to Transfer : " + cb4dto.getSubjectContainerNetwt());
			logger.debug("cb4dto.getSubjectContainerNetwt() =" + cb4dto.getSubjectContainerNetwt());
			logger.debug("cb4dto.getSourceTareWt() = " + cb4dto.getSourceTareWt());

			Float grossWeight = cb4dto.getSubjectContainerNetwt() + cb4dto.getSourceTareWt();
			logger.debug("Gross Weight = " + grossWeight);
			StringEntity params = new StringEntity("{   \"grossWeight\": {     \"value\": " + grossWeight + ",\"displayExponent\": -3,\"resolution\": -2 " +  "  } } ");
			jsonResult = getAPIResponse(url, "PATCH", params);
			logger.debug("Final weigh output done ");
			logger.debug("SOLVATE ..........jsonResult=" + jsonResult);
			updateQueue(jsonResult, cb4dto);
			return jsonResult;
		} catch (Exception e) {
			if (rs != null && !rs.isClosed())
				rs.close();
			if (prepStmt != null && !prepStmt.isClosed())
				prepStmt.close();
			//throw e;
			logger.debug("------------------------tracking the error.............." + e.getMessage());
			updateQueue(e.getMessage(), cb4dto);
			return e.getMessage();
		} finally {

		}
	}

	// sovlate
	private synchronized String solvateSample(cb4DTO cb4dto) throws Exception {

		logger.debug("solvate starts here....................");
		PreparedStatement prepStmt = null;
		ResultSet rs = null;
		String sourceLabwareItemId = "";
		String jsonResult = "";
		String sourceXPosition = "";
		String sourceYPosition = "";
		String url = this.mosaicAPIUrl; //"https://takeda.mosaic-cloud.com/api/Inventory";

		try {
			prepStmt = mosaicConnection.prepareStatement(FETCH_LABWAREITEMID_FOR_CONTAINER_BARCODE);
			prepStmt.setString(1, cb4dto.getSubjectContainerId());
			rs = prepStmt.executeQuery();
			if (rs.next()) {
				sourceLabwareItemId = rs.getString("LABWAREITEMID");
			}
			logger.debug("source labwareItemId..." + sourceLabwareItemId);
			prepStmt.close();
			rs.close();
			jsonResult = this.getSampleHolders(cb4dto.getSubjectContainerId().split(":")[0], cb4dto.getMosaicType());
			logger.debug("------------->>>>>" + jsonResult);
			
			jsonResult = jsonResult.substring(1, jsonResult.length() - 1);
			JSONObject obj = new JSONObject(jsonResult.toString());
			JSONArray sampleHolders = obj.getJSONArray("sampleHolders");
			logger.debug(sampleHolders);
			logger.debug("------------------");
			JSONObject sampleHolderIdentifier = new JSONObject(
					sampleHolders.getJSONObject(0).get("sampleHolderIdentifier").toString());
			JSONObject position = new JSONObject(sampleHolderIdentifier.get("position").toString());
			if (sourceLabwareItemId == null || sourceLabwareItemId.equals("")) {
				sourceLabwareItemId = sampleHolderIdentifier.get("labwareItemId").toString();
			}
			sourceXPosition = position.get("x").toString();
			sourceYPosition = position.get("y").toString();
			logger.debug("sourceXPosition = " + sourceXPosition + " sourceYPosition = " + sourceYPosition);
			url = url + "/SampleHolders/" + sourceLabwareItemId + "@(" + sourceXPosition + "," + sourceYPosition
					+ ")/AddSolventSample";

			int concentrationUnitType = 0;
			int concentrationDisplayExponent = 0;

			if (cb4dto.getSubjectContainerConcUnits() != null
					&& cb4dto.getSubjectContainerConcUnits().equalsIgnoreCase("uM")) {

				concentrationUnitType = 1;
				concentrationDisplayExponent = -3;
			}
			if (cb4dto.getSubjectContainerConcUnits() != null
					&& cb4dto.getSubjectContainerConcUnits().equalsIgnoreCase("mM")) {

				concentrationUnitType = 1;
				concentrationDisplayExponent = -3;
			}
			
			Double d = 0.0;
			int resolution = 0;
			if (cb4dto.getSolventAmount()!=null  && !cb4dto.getSolventAmount().equalsIgnoreCase("")) {
				d = Double.parseDouble(cb4dto.getSolventAmount());
			}
			BigDecimal b = BigDecimal.valueOf(d);		
			
			if ( b.scale()>=6 ) {
				String str = String.valueOf(d);
				String [] arrStr = str.split("\\.") ;
				String val = arrStr[1].substring(0, 5);
				logger.debug(val);
				String finalStrVal = arrStr[0]+"."+arrStr[1];
				d= Double.parseDouble(finalStrVal);
				logger.debug("final set value to 6 decimails" + d );
				logger.debug("greater than or equal to 6 so setting to 6 only : " + b.scale());
				resolution=-6;
				
			}else {
				logger.debug("less than than 6 so setting to same scale : "  + b.scale());
				resolution = b.scale()*-1;
			}
			
			logger.debug("resolution ... " + resolution); 
			logger.debug("final double value ... " + d); 
			
	
			StringEntity params = new StringEntity("{" + "    \"solvents\": [" + "        {"
					+ "            \"solventId\": 0," + "            \"proportion\": 1" + "        }" + "    ],"
					+ "    \"volume\": {" + "        \"value\": " + d + " ,"
					+ "        \"displayExponent\": " + concentrationDisplayExponent + ","
					+ "        \"resolution\": "+resolution+" ," + "        \"classification\": \"Legacy\"" + "    },"
					+ "    \"destinationConcentrationUnitType\":" + concentrationUnitType + " " + "}");

			jsonResult = getAPIResponse(url, "POST", params);
			logger.debug("SOLVATE ..........jsonResult=" + jsonResult);

			updateQueue(jsonResult, cb4dto);

			return jsonResult;
		} catch (Exception e) {
			if (rs != null && !rs.isClosed())
				rs.close();
			if (prepStmt != null && !prepStmt.isClosed())
				prepStmt.close();
			//throw e;
			logger.debug("------------------------tracking the error.............." + e.getMessage());
			updateQueue(e.getMessage(), cb4dto);
			return e.getMessage();
		} finally {

		}
	}

	// transfer Dry sample
	private synchronized String transferSample(cb4DTO cb4dto) throws Exception {

		logger.debug("start of the method transferSample..........");
		PreparedStatement prepStmt = null;
		ResultSet rs = null;
		String sourceLabwareItemId = "";
		String destinationLabwareItemId = "";
		String jsonResult = "";
		String sourceXPosition = "";
		String sourceYPosition = "";
		String destinationXPosition = "";
		String destinationYPosition = "";
		String url = this.mosaicAPIUrl; //"https://takeda.mosaic-cloud.com/api/Inventory";

		try {
			prepStmt = mosaicConnection.prepareStatement(FETCH_LABWAREITEMID_FOR_CONTAINER_BARCODE);
			prepStmt.setString(1, cb4dto.getSubjectContainerId().split(":")[0]);
			rs = prepStmt.executeQuery();
			if (rs.next()) {
				sourceLabwareItemId = rs.getString("LABWAREITEMID");
			}
			logger.debug("source labwareItemId..." + sourceLabwareItemId);
			prepStmt.close();
			rs.close();

			prepStmt = mosaicConnection.prepareStatement(FETCH_LABWAREITEMID_FOR_CONTAINER_BARCODE);
			prepStmt.setString(1, cb4dto.getObjectContainerId().split(":")[0]);
			rs = prepStmt.executeQuery();

			if (rs.next()) {
				destinationLabwareItemId = rs.getString("LABWAREITEMID");
			}
			logger.debug("Destination labwareItemId..." + destinationLabwareItemId);
			prepStmt.close();
			rs.close();

			jsonResult = this.getSampleHolders(cb4dto.getSubjectContainerId().split(":")[0], cb4dto.getMosaicType());
			logger.debug("-----jsonresult for the source barcode-------->>>>>" + jsonResult);
			jsonResult = jsonResult.substring(1, jsonResult.length() - 1);
			JSONObject obj = null;
			JSONArray sampleHolders = null;

			try {
				obj = new JSONObject(jsonResult.toString());
				sampleHolders = obj.getJSONArray("sampleHolders");
			} catch (Exception e) {
				logger.debug(
						">>>>>>>>>>>>>source barcode >>>>>>>>>>>>>>>>>>> ERROR >>>>>>>>>>>>>>." + e.getMessage());

				getSampleHolderForContainer(cb4dto, sourceLabwareItemId, "source");
			}

			logger.debug(" DESTINATION BARCODE SAMPLE HOLDER .. " + sampleHolders);

			logger.debug("--------source----------");
			JSONObject sampleHolderIdentifier = new JSONObject(
					sampleHolders.getJSONObject(0).get("sampleHolderIdentifier").toString());
			JSONObject position = new JSONObject(sampleHolderIdentifier.get("position").toString());
			if (sourceLabwareItemId == null || sourceLabwareItemId.equals("")) {
				sourceLabwareItemId = sampleHolderIdentifier.get("labwareItemId").toString();
			}
			logger.debug(position.get("x"));

			if (cb4dto.getSubjectContainerId().split(":").length > 1
					&& cb4dto.getSubjectContainerId().split(":")[1] != null) {
				String xy = cb4dto.getSubjectContainerId().split(":")[1];
				logger.debug(xy.length());
				int xpos=1;
				
				//
				if (xy.length()==4) {
					 xpos = Integer.parseInt(xy.substring(2, 3));
					sourceXPosition = xpos + "".trim();
					sourceYPosition = getValofPositionY(xy.substring(0, 2));
				}
				if (xy.length()==3) {
					 xpos = Integer.parseInt(xy.substring(1, 3));
					sourceXPosition = xpos + "".trim();
					sourceYPosition = getValofPositionY(xy.substring(0, 1));
				}
				//
				logger.debug(Integer.parseInt(sourceXPosition) + "::" + sourceYPosition);
			} else {
				sourceXPosition = position.get("x").toString();
				sourceYPosition = position.get("y").toString();
			}
			logger.debug("sourceXPosition = " + sourceXPosition + " sourceYPosition = " + sourceYPosition);

			logger.debug("cb4dto.getDestMosaicType()..........." + cb4dto.getDestMosaicType());

			String destinationJsonResult = this.getSampleHolders(cb4dto.getObjectContainerId().split(":")[0],
					cb4dto.getDestMosaicType());
			logger.debug("destinationJsonResult ..." + destinationJsonResult);
			logger.debug("######################################################################");

			destinationJsonResult = destinationJsonResult.substring(1, destinationJsonResult.length() - 1);
			logger.debug("destinationJsonResult ..." + destinationJsonResult);
			try {
				obj = new JSONObject(destinationJsonResult.toString());
				sampleHolders = obj.getJSONArray("sampleHolders");
				// sysout
			} catch (Exception e) {
				logger.debug(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ERROR >>>>>>>>>>>>>>." + e.getMessage());
				
				if (destinationLabwareItemId != null  && !destinationLabwareItemId.equals("")) {
					getSampleHolderForContainer(cb4dto, destinationLabwareItemId, "destination");
				}
				destinationJsonResult = this.getSampleHolders(cb4dto.getObjectContainerId().split(":")[0],
						cb4dto.getDestMosaicType());
				destinationJsonResult = destinationJsonResult.substring(1, destinationJsonResult.length() - 1);
				logger.debug("destinationJsonResult ..." + destinationJsonResult);
				obj = new JSONObject(destinationJsonResult.toString());
				sampleHolders = obj.getJSONArray("sampleHolders");
			}
			logger.debug(sampleHolders);
			logger.debug("--------destination----------");
			sampleHolderIdentifier = new JSONObject(
					sampleHolders.getJSONObject(0).get("sampleHolderIdentifier").toString());
			position = new JSONObject(sampleHolderIdentifier.get("position").toString());
			if (destinationLabwareItemId == null || destinationLabwareItemId.equals("")) {
				destinationLabwareItemId = sampleHolderIdentifier.get("labwareItemId").toString();
			}

			if (cb4dto.getObjectContainerId().split(":").length > 1
					&& (cb4dto.getObjectContainerId().split(":")[1] != null
							&& cb4dto.getObjectContainerId().split(":")[1] != "")) {
				String xy = cb4dto.getObjectContainerId().split(":")[1];
				logger.debug(xy.length());
				int xpos = 1 ;
//				Integer.parseInt(xy.substring(1, 3));
//				destinationXPosition = xpos + "".trim();
//				destinationYPosition = getValofPositionY(xy.substring(0, 1));
				
				
				//
				if (xy.length()==4) {
					 xpos = Integer.parseInt(xy.substring(2, 3));
					 destinationXPosition = xpos + "".trim();
					 destinationYPosition = getValofPositionY(xy.substring(0, 2));
				}
				if (xy.length()==3) {
					 xpos = Integer.parseInt(xy.substring(1, 3));
					 destinationXPosition = xpos + "".trim();
					 destinationYPosition = getValofPositionY(xy.substring(0, 1));
				}
				//
				
				
				logger.debug(Integer.parseInt(destinationXPosition) + "::" + destinationYPosition);
			} else {
				destinationXPosition = position.get("x").toString();
				destinationYPosition = position.get("y").toString();
			}
			// logger.debug(position.get("x"));
			// destinationXPosition = position.get("x").toString();
			// destinationYPosition = position.get("y").toString();
			logger.debug("destinationXPosition = " + destinationXPosition + " destinationYPosition = "
					+ destinationYPosition);

			String isDry = cb4dto.getTransferUnits();
			
			
			Double d = 0.0;
			int resolution = 0;
			if (cb4dto.getTransferAmount()!=null  && !cb4dto.getTransferAmount().equalsIgnoreCase("")) {
				d = Double.parseDouble(cb4dto.getTransferAmount());
			}						 	
			BigDecimal b = BigDecimal.valueOf(d);
//			logger.debug("Transfer amount " + b);
//			int resolution = b.scale();
//			logger.debug("before negative value .." + resolution);		
//			resolution=(resolution) * (-1);
//			logger.debug("resolution ... " + resolution); 
			
			if ( b.scale()>=6 ) {
				String str = String.valueOf(d);
				String [] arrStr = str.split("\\.") ;
				String val = arrStr[1].substring(0, 5);
				logger.debug(val);
				String finalStrVal = arrStr[0]+"."+arrStr[1];
				d= Double.parseDouble(finalStrVal);
				logger.debug("final set value to 6 decimails" + d );
				logger.debug("greater than or equal to 6 so setting to 6 only : " + b.scale());
				resolution=-6;
				
			}else {
				logger.debug("less than than 6 so setting to same scale : "  + b.scale());
				resolution = b.scale()*-1;
			}
			
			logger.debug("resolution ... " + resolution); 
			logger.debug("final double value ... " + d); 
			
			
			if (isDry.equalsIgnoreCase("mg")) {
				// https://example.com/api/Inventory/SampleHolders/12345@(1,1)/TransferNeatSample
				
				/// to do invoke above API.....
				String transferURL = url + "/SampleHolders/" + sourceLabwareItemId + "@(" + sourceXPosition + ","
						+ sourceYPosition + ")/TransferNeatSample";

				StringEntity params = new StringEntity("{" + "\"destination\": {" + "\"labwareItemId\":  "
						+ destinationLabwareItemId + "," + "    \"position\": {" + "      \"x\": "
						+ destinationXPosition + " ," + "      \"y\": " + destinationYPosition + " " + "    }" + "  },"
						+ "\"transferAmount\": {" + "\"value\": " + d + ","
						+ "\"displayExponent\": -3," + "\"resolution\": "+resolution+" ," + "\"classification\": \"Legacy\"" + "}"
						+ "}");
				
				logger.debug("params :" + params );
				
				jsonResult = getAPIResponse(transferURL, "POST", params);
				logger.debug(" json result after solution transfer ..! " + jsonResult);
			} else {

				int concentrationUnitType = 0;
				int concentrationDisplayExponent = 0;

				logger.debug(":cb4dto.getSubjectContainerConcUnits(): " + cb4dto.getSubjectContainerConcUnits());

				if (cb4dto.getSubjectContainerConcUnits() != null
						&& cb4dto.getSubjectContainerConcUnits().equalsIgnoreCase("uM")) {
					concentrationUnitType = 1;
					concentrationDisplayExponent =-3;  // -6;
				}
				if (cb4dto.getSubjectContainerConcUnits() != null
						&& cb4dto.getSubjectContainerConcUnits().equalsIgnoreCase("mM")) {
					concentrationUnitType = 1;
					concentrationDisplayExponent = -6;
				}
				if (cb4dto.getSubjectContainerConcUnits() != null
						&& cb4dto.getSubjectContainerConcUnits().equalsIgnoreCase("mg/mL")) {
					concentrationUnitType = 4;
					concentrationDisplayExponent = -3;
				}
				
				d = 0.0;
				if (cb4dto.getTransferAmount()!=null  && !cb4dto.getTransferAmount().equalsIgnoreCase("")) {
					d = Double.parseDouble(cb4dto.getTransferAmount());
				}						 	
				b = BigDecimal.valueOf(d);
				resolution = b.scale()*-1;
				logger.debug("resolution ... " + resolution); 
				
				String transferURL = url + "/SampleHolders/" + sourceLabwareItemId + "@(" + sourceXPosition + ","
						+ sourceYPosition + ")/TransferSolutionSample";
				StringEntity params = new StringEntity("{" + "  \"destination\": {" + "    \"labwareItemId\": "
						+ destinationLabwareItemId + " ," + "    \"position\": {" + "      \"x\": "
						+ destinationXPosition + " ," + "      \"y\": " + destinationYPosition + " " + "    }" + "  },"
						+ "  \"transferVolume\": {" + "    \"value\": " + d + " ,"
						+ "    \"displayExponent\": " + concentrationDisplayExponent + "," + "    \"resolution\": "+resolution+" ,"
						+ "    \"classification\": \"Measured\"" + "  }," + "  "
						+ "\"destinationConcentrationUnitType\": " + concentrationUnitType + " " + "}");

				jsonResult = getAPIResponse(transferURL, "POST", params);
				logger.debug(" json result after solution transfer ..! " + jsonResult);
			}

			updateQueue(jsonResult, cb4dto);

			return jsonResult;
		} catch (Exception e) {
			if (rs != null && !rs.isClosed())
				rs.close();
			if (prepStmt != null && !prepStmt.isClosed())
				prepStmt.close();
			
			String sLabwareType = null;
			String dLabwareType= null;
				
			
			prepStmt = mosaicConnection.prepareStatement("select labwaretypeid from inv_labware_type where upper(labwaretypename)=upper(?) ");
			prepStmt.setString(1, cb4dto.getMosaicType());
			rs = prepStmt.executeQuery();
			if (rs.next()) {
				sLabwareType = rs.getString("labwaretypeid");
			}
			logger.debug("source labwareItemId..." + sLabwareType);
			prepStmt.close();
			rs.close();
			
			prepStmt = mosaicConnection.prepareStatement("select labwaretypeid from inv_labware_type where upper(labwaretypename)=upper(?) ");
			prepStmt.setString(1, cb4dto.getDestMosaicType());
			rs = prepStmt.executeQuery();
			if (rs.next()) {
				dLabwareType = rs.getString("labwaretypeid");
			}
			logger.debug("dest labwareItemId..." + dLabwareType);
			prepStmt.close();
			rs.close();
			
			//throw e;
			logger.debug("------------------------tracking the error.............." + e.getMessage());
			updateQueue(e.getMessage() + " Source Barcode :  " + cb4dto.getSubjectContainerId() +" source Mosaic Type From CB4 : " + cb4dto.getMosaicType() +
		     " source labware type in Mosaic : " + sLabwareType + 
	 		 " Dest Barcode : " + cb4dto.getObjectContainerId() +" Destination Mosaic Type From CB4 : " + cb4dto.getDestMosaicType() + " Dest labware type in Mosaic : " + dLabwareType + " ", cb4dto);
			return e.getMessage();
		} finally {

		}
	}

	private synchronized String disposeLabwareItem(cb4DTO cb4dto) throws Exception {

		PreparedStatement prepStmt = null;
		ResultSet rs = null;
		String labwareItemId = "";
		String jsonResult = "";
		String barcode = "";
		String url = this.mosaicAPIUrl ; //"https://takeda.mosaic-cloud.com/api/Inventory";

		try {
			prepStmt = mosaicConnection.prepareStatement(FETCH_LABWAREITEMID_FOR_CONTAINER_BARCODE);
			
			if (cb4dto.getSubjectContainerId()!=null && !cb4dto.getSubjectContainerId().equals("")) {
				barcode = cb4dto.getSubjectContainerId().split(":")[0]; 
			}else {
				barcode = cb4dto.getSubjectCollectionId();
			}
			
			//prepStmt.setString(1, cb4dto.getSubjectContainerId().split(":")[0]);
			System.out.println("Collection id : " + barcode );
			prepStmt.setString(1, barcode);
			rs = prepStmt.executeQuery();

			if (rs.next()) {
				labwareItemId = rs.getString("LABWAREITEMID");
			}
			logger.debug(labwareItemId);
			url = url + "/LabwareItems/" + labwareItemId + "/Dispose";
			logger.debug("Dispose URL : " + url);
			jsonResult = getAPIResponse(url, "POST", null);
			logger.debug(labwareItemId + " has been sucessfully Disposed...");
			updateQueue(jsonResult, cb4dto);
			return jsonResult;
		} catch (Exception e) {
			if (rs != null && !rs.isClosed())
				rs.close();
			if (prepStmt != null && !prepStmt.isClosed())
				prepStmt.close();
			//throw e;
			logger.debug("------------------------tracking the error.............." + e.getMessage());
			updateQueue(e.getMessage(), cb4dto);
			return e.getMessage();
		} finally {
			prepStmt.close();
			rs.close();
		}
		
	}

	public int getSetId (String setName) {
		PreparedStatement prepStmt = null;
		ResultSet rs = null;
		String qry = "select setid from INV_SET where upper(setname)= upper(?)";
		int setId=0;
		try {
			prepStmt = mosaicConnection.prepareStatement(qry);
			prepStmt.setString(1, setName);
			rs = prepStmt.executeQuery();
			if (rs.next()) {
				setId=rs.getInt("setid");
			}
			prepStmt.close();
			rs.close();
		} catch (Exception e) {
			logger.debug(e.getMessage());
		}
		return setId;
	}
	
	
	private synchronized String addToSet(cb4DTO cb4dto) throws Exception {
		logger.debug("inside addtoSet method");
		PreparedStatement prepStmt = null;
		ResultSet rs = null;
		String labwareItemId = "";
		String jsonResult = "";
		String url = this.mosaicAPIUrl+"/LabwareItems/";     // "https://takeda.mosaic-cloud.com/api/Inventory/LabwareItems/";
		String barcode="";
		int setId=0;
		try {
			prepStmt = mosaicConnection.prepareStatement("select il.LABWAREITEMID as LABWAREITEMID from inv_labware_item il where il.labwarebarcode=?");
			
			if ( cb4dto.getSubjectContainerId()!=null)
				barcode=cb4dto.getSubjectContainerId().split(":")[0];
			else 
				barcode=cb4dto.getSubjectCollectionId().split(":")[0];
			
			logger.debug("barcode..." + barcode);
			prepStmt.setString(1, barcode);
			rs = prepStmt.executeQuery();
			if (rs.next()) {
				labwareItemId = rs.getString("LABWAREITEMID");
				logger.debug("labwareitemid... " + labwareItemId );
			}
			logger.debug(labwareItemId);
			url = url + labwareItemId ;
			logger.debug("add To Set URL : " + url);
			setId = getSetId(cb4dto.getSetName());
			StringEntity params = new StringEntity("{"+"\"setId\": "+setId+"}");
			jsonResult = getAPIResponse(url, "PATCH", params);
			logger.debug(labwareItemId + " has been sucessfully SET..." + setId);
			updateQueue(jsonResult, cb4dto);
			return jsonResult;

		} catch (Exception e) {
			if (rs != null && !rs.isClosed())
				rs.close();
			if (prepStmt != null && !prepStmt.isClosed())
				prepStmt.close();
			//throw e;
			logger.debug("------------------------tracking the error.............." + e.getMessage());
			updateQueue(e.getMessage(), cb4dto);
			return e.getMessage();
		} finally {
			prepStmt.close();
			rs.close();
		}
		
	}
	
	public  ArrayList<cb4DTO> fetchModifiedSampleData(Connection connection) throws Exception {
		logger.debug("started at : " + watch.start());
		PreparedStatement prepStmt = null;
		ResultSet rs = null;
		cb4DTO cb4dto = null;
		ArrayList<cb4DTO> list = new ArrayList<cb4DTO>();
		try {
			prepStmt = platoConnection.prepareStatement(FETCH_MODIFIED_SAMPLE_DATA);
			rs = prepStmt.executeQuery();
			rs.setFetchSize(1000);
		//	logger.debug("----------------------------------------------");
			System.out.println("before while...");
			while (rs.next()) {

				cb4dto = new cb4DTO();
				cb4dto.setOperationSid(rs.getInt("OPERATION_SID"));
				cb4dto.setOperationType(rs.getString("OPERATION_TYPE"));
				cb4dto.setSubjectContainerSid(rs.getInt("SUBJECT_CONTAINER_SID"));
				cb4dto.setSubjectContainerId(rs.getString("SUBJECT_CONTAINER_ID"));
				cb4dto.setObjectContainerSid(rs.getInt("OBJECT_CONTAINER_SID"));
				cb4dto.setObjectContainerId(rs.getString("OBJECT_CONTAINER_ID"));
				cb4dto.setTransferAmount(rs.getString("TRANSFER_AMOUNT") );				
				cb4dto.setTransferUnits(rs.getString("TRANSFER_UNITS"));
				cb4dto.setSolventAmount(rs.getString("SOLVENT_AMOUNT"));
				cb4dto.setSolventUnits(rs.getString("SOLVENT_UNITS"));
				cb4dto.setSubjectContainerConc(rs.getFloat("SUBJECT_CONTAINER_CONC"));
				cb4dto.setSubjectContainerConcUnits(rs.getString("SUBJECT_CONTAINER_CONC_UNITS"));
				cb4dto.setObjectContainerConc(rs.getFloat("OBJECT_CONTAINER_CONC"));
				cb4dto.setObjectContainerConcUnits(rs.getString("OBJECT_CONTAINER_CONC_UNITS"));
				cb4dto.setActionDate(rs.getDate("ACTION_DATE"));
				cb4dto.setActionText(rs.getString("ACTION_TEXT"));
				cb4dto.setActionLocation(rs.getString("ACTION_LOCATION"));
				cb4dto.setSubjectContainerNetwt(rs.getFloat("SUBJECT_CONTAINER_NETWT"));
				cb4dto.setSubjectContainerNetwtUnits(rs.getString("SUBJECT_CONTAINER_NETWT_UNITS"));
				cb4dto.setMosaicType(rs.getString("sourceType"));
				cb4dto.setDestMosaicType(rs.getString("destType"));
				cb4dto.setSourceTareWt(rs.getFloat("source_tarewt"));
				cb4dto.setDestTareWt(rs.getFloat("dest_tarewt"));
				cb4dto.setSourceUOM(rs.getString("source_uom"));
				cb4dto.setDestUOM(rs.getString("dest_uom"));
				cb4dto.setSubjectCollectionId(  rs.getString("subject_collection_id"));
				cb4dto.setSetName(  rs.getString("set_name"));
				cb4dto.setCompoundId( rs.getString("compound_id"));
				cb4dto.setSubjectContainerVolume( rs.getString("subject_container_volume"));
				cb4dto.setSolvent(rs.getString("solvent"));
				

				list.add(cb4dto);
			}

			//logger.debug("----------------------------------------------");
			logger.debug("List size : " + list.size() );
			logger.debug("started at : " + watch.time(TimeUnit.SECONDS));
			prepStmt.close();
			rs.close();
			// logger.debug("List of Records to be pushed to CB4" + list);
		} catch (Exception e) {
			if (rs != null && !rs.isClosed())
				rs.close();
			if (prepStmt != null && !prepStmt.isClosed())
				prepStmt.close();
			throw e;
		} finally {

		}

		return list;

	}

	
	
	public String getMosaicLabwareType(String barcode) throws Exception {
		String mosaicType="";
		String qry = "select cc.container_collection_sid, cc.barcode, cct.mosaic_type as mosaic_plate_or_rack_type, "
				+ " c.container_sid, c.container_id, ct.mosaic_type as mosaic_tube_type "
				+ " from plato.container c, plato.container_collection cc, plato.lu_container_type ct,"
				+ " plato.lu_cont_coll_type cct "
				+ " where c.container_type_sid= ct.container_type_sid and "
				+ " c.container_collection_sid = cc.container_collection_sid(+) and "
				+ " cc.cont_coll_type_sid = cct.cont_coll_type_sid(+) and c.container_id  in (?)";
		
		PreparedStatement prepStmt = null;
		ResultSet rs = null;
		try {
			prepStmt = platoConnection.prepareStatement(qry);
			prepStmt.setString(1, barcode.trim());
			rs = prepStmt.executeQuery();
			if(rs.next()) {
				mosaicType=rs.getString("mosaic_plate_or_rack_type");
			}
			prepStmt.close();
			rs.close();
		} catch (Exception e) {
			logger.debug(e.getMessage());
			prepStmt.close();
			rs.close();
		}
		return mosaicType;
		
	}
	
	
	
	/*
	 * for solvate when no sample holder found then create a labwareitemid with
	 * associated type (from the container table of plato, with the api curl
	 * --location --request POST
	 * 'https://takeda.mosaic-cloud.com/api/Inventory/LabwareItems' \ --header
	 * 'Authorization: Basic TW9zYWljQVBJVVNFUjpNMHNhaWNBUElAMTAw' \ --header
	 * 'Content-Type: application/json' \ --data-raw '[{ "barcode": "B1013282",
	 * "labwareTypeId": 12112 }]' this api also creates sample holder and then fire
	 * again the sample holder api to get x and y positions.
	 * 
	 */
	public String getSampleHolders(String barcode, String labwareTypeName) throws Exception {
		logger.debug("barcode : " + barcode + " labwareTypeName:  " + labwareTypeName);

		int mosaicLabwaretypeId = 0;
		PreparedStatement prepStmt = null;
		ResultSet rs = null;
		String output = "";
		JSONObject obj = null;
		String labwareItemIdCreated = "";
		String url = this.mosaicAPIUrl+"/LabwareItems"; //"https://takeda.mosaic-cloud.com/api/Inventory/LabwareItems";
		String mosaic_plate_or_rack_type="";
		try {
			output = getAPIResponse(url + "?barcodes=" + barcode + "&expand=sampleHolders", "GET", null);
			logger.debug("output...for solvate is ................19th may" + output);

			if (output.equals("[]")) {
				// logger.debug("No sample holder associated to this barcode...Need to
				// create");
				logger.debug("inside if since we did not have records...." + labwareTypeName);
				prepStmt = mosaicConnection.prepareStatement(
						"select labwaretypeid from inv_labware_type where upper(labwaretypename)=upper(?) ");
				logger.debug(prepStmt);
				prepStmt.setString(1, labwareTypeName.trim());
				rs = prepStmt.executeQuery();

				if (rs.next()) {
					mosaicLabwaretypeId = rs.getInt("labwaretypeid");
				}
				logger.debug("mosaicLabwaretypeId..." + mosaicLabwaretypeId);
				
				/*
				 * if mosaicLabwaretypeId is 0 then apply logic here to get labaretypeid
				 */
				
				if (mosaicLabwaretypeId==0) { 
				
					mosaic_plate_or_rack_type= getMosaicLabwareType(barcode);					
					prepStmt = mosaicConnection.prepareStatement(
							"select labwaretypeid from inv_labware_type where upper(labwaretypename)=upper(?) ");
					logger.debug(prepStmt);
					prepStmt.setString(1, labwareTypeName.trim());
					rs = prepStmt.executeQuery();
					if (rs.next()) {
						mosaicLabwaretypeId = rs.getInt("labwaretypeid");
					}
					logger.debug("after checking next level the mosaicLabwaretypeId is :" + mosaicLabwaretypeId);
					prepStmt.close();
					rs.close();
				}
				
//				StringEntity params = new StringEntity(
//						"[{   \"barcode\": \"" + barcode + "\",   \"labwareTypeId\": " + mosaicLabwaretypeId + "  }]");
				
				String lab = "\\TAKEDA\\TSD\\S.M. Lab";
				String replacedValue = lab.replace("\\","\\\\");
				StringEntity params =  new StringEntity("[{\"barcode\": \"" + barcode + "\",\"labwareTypeId\":" + mosaicLabwaretypeId + ",\"location\":{\"locationIdentifier\":{\"locationPath\": \""+replacedValue+ "\"}}}]");
				
				
				
				labwareItemIdCreated = getAPIResponse(url, "POST", params);
				logger.debug(" to get the labware item id after created with api " + labwareItemIdCreated);
				obj = new JSONObject(labwareItemIdCreated.toString());
				try {
					String sourceLabwareItemId = obj.get("labwareItemIds").toString().replace("[", "").replace("]", "");
					logger.debug("output " + sourceLabwareItemId);
					if (sourceLabwareItemId != null && !sourceLabwareItemId.equalsIgnoreCase("")) {
						String finalOutput = getAPIResponse(
								this.mosaicAPIUrl+"/LabwareItems?barcodes=" + barcode
										+ "&expand=sampleHolders",
								"GET", null);
						logger.debug("finalOutput " + finalOutput);
						output = finalOutput;
					}
					prepStmt.close();
					rs.close();
				} catch (Exception e) {
					logger.debug(" error :" + e.getMessage());
					prepStmt.close();
					rs.close();
				}
			}

		} catch (Exception e) {
			// TODO: handle exception
		} finally {

		}

		return output;
	}

	private void updateQueue(String jsonResult, cb4DTO cb4dto) throws Exception {
		PreparedStatement prepStmt = null;
		try {

			String result = jsonResult.equalsIgnoreCase("[]") ? "success" : jsonResult;
			prepStmt = platoConnection.prepareStatement("update plato.mosaic_operation_queue "
					+ "set processed_date=current_timestamp, result_text=?,status=? where operation_sid=?");
			logger.debug(
					"before updating mosaic_operation_queue " + cb4dto.getOperationSid() + "Message : " + jsonResult);
			prepStmt.setString(1, result);
			prepStmt.setString(2, "processed");
			prepStmt.setInt(3, cb4dto.getOperationSid());

			prepStmt.executeUpdate();
			logger.debug(
					"updated back Queue Table with result and current time stamp for the processed records.......");
			prepStmt.close();
		} catch (Exception e) {
			e.printStackTrace();
			prepStmt.close();
		} finally {

		}

	}
	
	
	
	public void updateQueueForUpdate( int operationSid, String result) throws Exception {
		PreparedStatement prepStmt = null;
		try {

			prepStmt = platoConnection.prepareStatement("update plato.mosaic_operation_queue "
					+ "set processed_date=current_timestamp, result_text=?,status=? where operation_sid=?");
			prepStmt.setString(1, result);
			prepStmt.setString(2, "processed");
			prepStmt.setInt(3, operationSid);			
			int update = prepStmt.executeUpdate();
			logger.debug("updated back " + result);
		//	platoConnection.commit();
			 prepStmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	
	// for UPDATE, need to look for v_container with the barcode and get the data.

	public PlatoContainer getPlatContainerDetails() throws Exception {

		String FETCH_PLATO_CONTAINER_DETAILS = "select ct.CONTAINER_SID ,ct.CONTAINER_COLLECTION_SID ,ct.CONTAINER_TYPE_SID ,ct.ORDER_ITEM_SID ,"
				+ " ct.CONTAINER_STATUS_SID , ct.CONTAINER_ID ,ct.WELL_POS ,ct.CMPD_CORP_ID ,ct.SAMPLE_STATE ,"
				+ " ct.CONC ,ct.CONC_UOM ,ct.VOLUME ,ct.VOLUME_UOM , ct.TARE_WT ,ct.NET_WT ,ct.MASS_UOM ,ct.DILUENT_TEXT ,"
				+ " ct.SOURCE_CONTAINER_SID ,ct.LOCATION_SID ,ct.CONTAINER_USAGE_SID , ct.FRZ_THW_COUNT ,ct.SOLUBILITY_STATUS_SID ,"
				+ " ct.BATCH_SAMPLE_ID ,ct.CMPD_REMAPPED ,ct.OP_STATUS_CODE ,ct.OP_STATUS_DESC , ct.TRANSACTION_GUID ,ct.CREATED_DATE , "
				+ " ct.CREATED_BY ,ct.UPDATED_DATE ,ct.UPDATED_BY ,ct.NOTES ,ct.SERIES_INDEX , ct.CMPD_MIXTURE ,ct.SAMPLE_STATE_DATE ,"
				+ " ct.PURPOSE ,ct.STORAGE_TEMPERATURE , ctyp.container_type_id as ctypeid, ctyp.mosaic_type mosaictype"
				+ " FROM PLATO.CONTAINER ct join PLATO.LU_CONTAINER_TYPE ctyp on ct.container_type_sid = ctyp.container_type_sid "
				+ " WHERE ct.CONTAINER_ID= ? ";
		PreparedStatement prepStmt = null;
		ResultSet rs = null;
		PlatoContainer container = new PlatoContainer();
		try {
			prepStmt = platoConnection.prepareStatement(FETCH_PLATO_CONTAINER_DETAILS);
			rs = prepStmt.executeQuery();
			if (rs.next()) {

				container.setContainerSid(rs.getLong("CONTAINER_SID"));
				container.setContainerCollectionSid(rs.getLong("CONTAINER_COLLECTION_SID"));
				container.setContainerTypeSid(rs.getLong("CONTAINER_TYPE_SID"));
				container.setContainerStatusSid(rs.getInt("CONTAINER_STATUS_SID"));
				container.setContainerId(rs.getString("CONTAINER_ID"));
				container.setWellPos(rs.getString("WELL_POS"));
				container.setCmpdCorpId(rs.getString("CMPD_CORP_ID"));
				container.setSampleState(rs.getString("SAMPLE_STATE"));
				container.setConc(rs.getFloat("CONC"));
				container.setConcUOM(rs.getString("CONC_UOM"));
				container.setVolume(rs.getFloat("VOLUME"));
				container.setVolumeUOM(rs.getString("VOLUME_UOM"));
				container.setTareWt(rs.getFloat("TARE_WT"));
				container.setNetWt(rs.getFloat("NET_WT"));
				container.setMassUOM(rs.getString("MASS_UOM"));
				container.setDiluentText(rs.getString("DILUENT_TEXT"));
				container.setContainerSid(rs.getLong("SOURCE_CONTAINER_SID"));
				container.setLocationId(rs.getString("LOCATION_SID"));
				container.setContainerUsageSid(rs.getLong("CONTAINER_USAGE_SID"));
				container.setCreatedDate(rs.getTimestamp("CREATED_DATE"));
				container.setUpdatedDate(rs.getTimestamp("UPDATED_DATE"));
				// container.sett ;rs.getString("FRZ_THW_COUNT") );
				// container.setsta rs.getString("SOLUBILITY_STATUS_SID") );
				// rs.getString("BATCH_SAMPLE_ID") );
				// container.setO(rs.getString("ORDER_ITEM_SID") );
				// container.setcrs.getString("CMPD_REMAPPED") );
				// rs.getString("OP_STATUS_CODE") );
				// (rs.getString("OP_STATUS_DESC") );
				// container.setttrs.getString("TRANSACTION_GUID") );
				// rs.getString("CREATED_BY") );
				// rs.getString("UPDATED_BY") );
				// rs.getString("NOTES") );
				// container.setse rs.getString("SERIES_INDEX") );
				// container.setcm rs.getString("CMPD_MIXTURE") );
				// getString("SAMPLE_STATE_DATE") );
				// rs.getString("PURPOSE") );
				// rs.getString("STORAGE_TEMPERATURE") );
				// container.setContainerTypeId(rs.getString("ctypeid") );
				// rs.getString("mosaictype") );
			}
			prepStmt.close();
			rs.close();
		} catch (Exception e) {
			// TODO: handle exception
			prepStmt.close();
			rs.close();
		} finally {

		}
		return container;

	}

	public SampleHolderData getSampleHolderForContainer(cb4DTO cb4dto, String labwareitemid, String isSource)
			throws SQLException {

		logger.debug("Inside getSampleHolderForContainer : Labid :" + labwareitemid + " isSource " + isSource);

		SampleHolderData sampleHolderData = null;
		MosaicInvRegCompound mosaicInvRegCompound = null;
		PlatoContainer platoContainer = null;
		try {
			Properties cb4SolventMap;

			if (isSource.equalsIgnoreCase("source")) {
				logger.debug("cb4dto.getSubjectContainerId() ............." + cb4dto.getSubjectContainerId());
				platoContainer = getPlatContainerDetailsForContainerId(cb4dto.getSubjectContainerId());
			} else if (isSource.equalsIgnoreCase("destination")) {
				logger.debug("cb4dto.getObjectContainerId() ............." + cb4dto.getObjectContainerId());
				platoContainer = getPlatContainerDetailsForContainerId(cb4dto.getObjectContainerId());
			}

			// ----------------------------------------------------------------------------------
			logger.debug("platoContainer.getCmpdCorpId() :" + platoContainer.getCmpdCorpId());
			if (platoContainer.getCmpdCorpId() != null) {
				String namepart0 = null;
				String namepart1 = null;
				String namepart2 = null;
				String namepart3 = null;
				String[] nameparts = platoContainer.getCmpdCorpId().split("-");

				if (platoContainer.getCmpdCorpId().toUpperCase().startsWith("TR")) {
					namepart0 = nameparts[0];
					namepart1 = nameparts[1];

				} else if (platoContainer.getCmpdCorpId().toUpperCase().startsWith("TAK-")) {
					namepart0 = nameparts[0] + "-" + nameparts[1];
					namepart1 = nameparts[2];

				} else if (platoContainer.getCmpdCorpId().toUpperCase().startsWith("BIO")) {
					// namepart2 = tbosDSISSampleDetail.getSampleCorpId();
					String[] nameparts2 = platoContainer.getCmpdCorpId().split("_");
					namepart2 = nameparts2[0];
					namepart3 = nameparts2[1];
				}
				// throw new Exception("Can't handle " +
				// tbosDSISSampleDetail.getSampleCorpId());
				MosaicCompoundService mosaicCompoundService = new MosaicCompoundService();
				mosaicInvRegCompound = mosaicCompoundService.getInvRegCompoundByNamepart012(mosaicConnection, namepart0,
						namepart1, namepart2, namepart3);
				logger.debug("mosaicInvRegCompound .. 1 " + mosaicInvRegCompound);
				// -----------------------------------------------------------------------------------
			}

			String parentLocation = "\\TAKEDA\\TSD";
			cb4SolventMap = new Properties();
			cb4SolventMap.put("OTHER", "N/A");
			cb4SolventMap.put("DMSO", "DMSO");
			cb4SolventMap.put("ACN", "N/A");
			cb4SolventMap.put("WATER", "Water");
			cb4SolventMap.put("DMSO_ACN", "N/A");
			cb4SolventMap.put("D6", "d6-DMSO");
			cb4SolventMap.put("DMSO_10mMTFA", "DMSO_10mMTFA");

			MosaicInvSolventService mosaicInvSolventService = new MosaicInvSolventService();
			HashMap<String, MosaicInvSolvent> mosaicInvSolventMap = mosaicInvSolventService
					.getAllSolvents(mosaicConnection);
			String solventName = platoContainer.getDiluentText();

			if (solventName != null) {
				String temp = cb4SolventMap.getProperty(solventName);

				if (temp != null) {
					solventName = temp;
				} else {
					throw new Exception("Solvent " + solventName + " was not found in Mosaic.");
				}
			}

			if (platoContainer.getCmpdCorpId() != null) {
				if (platoContainer.getSampleState().equalsIgnoreCase("WET")) {
					sampleHolderData = prepareSampleHolderData(parentLocation, platoContainer.getSampleState(),
							solventName, mosaicInvSolventMap, platoContainer.getVolume(), platoContainer.getVolume(),
							platoContainer.getConc(), platoContainer.getConcUOM());
				} else if (platoContainer.getSampleState().equalsIgnoreCase("DRY")) {
					sampleHolderData = prepareSampleHolderData(parentLocation, platoContainer.getSampleState(),
							solventName, mosaicInvSolventMap, platoContainer.getNetWt(), platoContainer.getNetWt(),
							platoContainer.getConc(), platoContainer.getConcUOM());

				}
			}
		} catch (Exception e) {
			// TODO: handle exception
		}

		logger.debug("sampleHolderData...............   " + sampleHolderData);
		long labid=0;
		if (labwareitemid!=null && !labwareitemid.equals("")) {
			labid=Long.parseLong(labwareitemid);
		}
		com.takeda.rds.mosaic.service.MosaicInvSampleHolderService invSampleHolderService = new MosaicInvSampleHolderService();
		invSampleHolderService.insertUpdateInvSampleHolder(mosaicConnection, sampleHolderData.getIsNeat(),
				sampleHolderData.getAvailableAmount(), sampleHolderData.getAmount(),
				sampleHolderData.getAmountResolution(), sampleHolderData.getAmountDisplayExponent(),
				sampleHolderData.getConc(), null, mosaicInvRegCompound.getCompoundId(),
				sampleHolderData.getConcentrationUnitType(), sampleHolderData.getConcentrationDisplayExponent(),
				sampleHolderData.getSolventId(), sampleHolderData.getSolventProportion(), labid,
				1, 1);

		return sampleHolderData;

		// -----------------------------------------------------
	}

	private String getValofPositionY(String a) {

		HashMap map = new HashMap();
		map.put("A", 1);
		map.put("B", 2);
		map.put("C", 3);
		map.put("D", 4);
		map.put("E", 5);
		map.put("F", 6);
		map.put("G", 7);
		map.put("H", 8);
		map.put("I", 9);
		map.put("J", 10);
		map.put("K", 11);
		map.put("L", 12);
		map.put("M", 13);
		map.put("N", 14);
		map.put("O", 15);
		map.put("P", 16);
		map.put("Q", 17);
		map.put("R", 18);
		map.put("S", 19);
		map.put("T", 20);
		map.put("U", 21);
		map.put("V", 22);
		map.put("W", 23);
		map.put("X", 24);
		map.put("Y", 25);
		map.put("Z", 26);
		map.put("AA", 27);
		map.put("AB", 28);
		map.put("AC", 29);
		map.put("AD", 30);
		map.put("AE", 31);
		map.put("AF", 32);

		return map.get(a).toString();

	}

	public PlatoContainer getPlatContainerDetailsForContainerId(String containerId) throws Exception {

		logger.debug("CONTAINER ID ...TO FETCH ITS DETAILS: " + containerId);
		String FETCH_PLATO_CONTAINER_DETAILS = "select ct.CONTAINER_SID ,ct.CONTAINER_COLLECTION_SID ,ct.CONTAINER_TYPE_SID ,ct.ORDER_ITEM_SID ,"
				+ " ct.CONTAINER_STATUS_SID , ct.CONTAINER_ID ,ct.WELL_POS ,ct.CMPD_CORP_ID ,ct.SAMPLE_STATE ,"
				+ " ct.CONC ,ct.CONC_UOM ,ct.VOLUME ,ct.VOLUME_UOM , ct.TARE_WT ,ct.NET_WT ,ct.MASS_UOM ,ct.DILUENT_TEXT ,"
				+ " ct.SOURCE_CONTAINER_SID ,ct.LOCATION_SID ,ct.CONTAINER_USAGE_SID , ct.FRZ_THW_COUNT ,ct.SOLUBILITY_STATUS_SID ,"
				+ " ct.BATCH_SAMPLE_ID ,ct.CMPD_REMAPPED ,ct.OP_STATUS_CODE ,ct.OP_STATUS_DESC , ct.TRANSACTION_GUID ,ct.CREATED_DATE , "
				+ " ct.CREATED_BY ,ct.UPDATED_DATE ,ct.UPDATED_BY ,ct.NOTES ,ct.SERIES_INDEX , ct.CMPD_MIXTURE ,ct.SAMPLE_STATE_DATE ,"
				+ " ct.PURPOSE ,ct.STORAGE_TEMPERATURE , ctyp.container_type_id as ctypeid, ctyp.mosaic_type mosaictype"
				+ " FROM PLATO.CONTAINER ct join PLATO.LU_CONTAINER_TYPE ctyp on ct.container_type_sid = ctyp.container_type_sid "
				+ " WHERE ct.CONTAINER_ID= ? ";
		PreparedStatement prepStmt = null;
		ResultSet rs = null;
		PlatoContainer container = new PlatoContainer();
		try {
			prepStmt = platoConnection.prepareStatement(FETCH_PLATO_CONTAINER_DETAILS);
			prepStmt.setString(1, containerId);
			rs = prepStmt.executeQuery();
			if (rs.next()) {

				container.setContainerSid(rs.getLong("CONTAINER_SID"));
				container.setContainerCollectionSid(rs.getLong("CONTAINER_COLLECTION_SID"));
				container.setContainerTypeSid(rs.getLong("CONTAINER_TYPE_SID"));
				container.setContainerStatusSid(rs.getInt("CONTAINER_STATUS_SID"));
				container.setContainerId(rs.getString("CONTAINER_ID"));
				container.setWellPos(rs.getString("WELL_POS"));
				container.setCmpdCorpId(rs.getString("CMPD_CORP_ID"));
				container.setSampleState(rs.getString("SAMPLE_STATE"));
				container.setConc(rs.getFloat("CONC"));
				container.setConcUOM(rs.getString("CONC_UOM"));
				container.setVolume(rs.getFloat("VOLUME"));
				container.setVolumeUOM(rs.getString("VOLUME_UOM"));
				container.setTareWt(rs.getFloat("TARE_WT"));
				container.setNetWt(rs.getFloat("NET_WT"));
				container.setMassUOM(rs.getString("MASS_UOM"));
				container.setDiluentText(rs.getString("DILUENT_TEXT"));
				container.setContainerSid(rs.getLong("SOURCE_CONTAINER_SID"));
				container.setLocationId(rs.getString("LOCATION_SID"));
				container.setContainerUsageSid(rs.getLong("CONTAINER_USAGE_SID"));
				container.setCreatedDate(rs.getTimestamp("CREATED_DATE"));
				container.setUpdatedDate(rs.getTimestamp("UPDATED_DATE"));
				container.setContainerTypeId(rs.getString("mosaictype"));
				
			}
			prepStmt.close();
			rs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return container;

	}

	private int  getTheQueueCountBeforeBatchProcess () throws SQLException  {
		int count =0;
		PreparedStatement prepStmt = null;
		ResultSet rs = null;
		String countQuery = "select count(*) as cnt from PLATO.v_mosaic_operation_queue";
		try {
			prepStmt = platoConnection.prepareStatement(countQuery);
			rs = prepStmt.executeQuery();
			if(rs.next()) {
				count = rs.getInt("cnt");
			}
			prepStmt.close();
			rs.close();
		} catch (Exception e) {
			e.printStackTrace();
			prepStmt.close();
			rs.close();
		}
		return count;
	}
	
	private boolean checkIfLabwareExistsForMove(String barcode) throws Exception{
		
		boolean isLabwareItemExists = false;
		PreparedStatement prepStmt = null;
		ResultSet rs = null;
		String containerId = "";
		String labwareitemid = "";
		try {
			String substanceQry = "select labwareitemid from inv_labware_item where  LABWAREBARCODE=?";
			if (barcode!=null && !barcode.equalsIgnoreCase("")) {
				containerId = barcode.split(":")[0];
			}
			prepStmt = mosaicConnection.prepareStatement(substanceQry);
			prepStmt.setString(1, containerId);
			rs = prepStmt.executeQuery();
			if (rs.next()) {
				labwareitemid = rs.getString("labwareitemid");
			}
			logger.debug("labwareitemid check before submit..." + labwareitemid);
			prepStmt.close();
			rs.close();
		
			isLabwareItemExists = !labwareitemid.equalsIgnoreCase("") ? true : false;
			
		} catch (Exception e) {
			e.printStackTrace();
			prepStmt.close();
			rs.close();
		}
		return isLabwareItemExists;
		
		
	}
	
	private boolean checkIfSubstanceAlreadyExists(String barcode) throws Exception{
				
		boolean isSubstanceExists = false;
		PreparedStatement prepStmt = null;
		ResultSet rs = null;
		String substanceid="";
		String sourceXPosition = "1";
		String sourceYPosition="1";
		String qry = "select sh.substanceid from inv_sample_holder sh, inv_labware_item ilt " + 
				"where sh.labwareitemid=ilt.labwareitemid " + 
				"and ilt.labwarebarcode= ? and sh.xposition=? " + 
				"and sh.yposition=? ";
		try {
			
			
			String [] barcodeArr =  barcode.split(":");
			
			if (barcodeArr.length > 1 && barcodeArr[1] != null) {
				String xy = barcodeArr[1];
				logger.debug(xy.length());
				int xpos =1; // Integer.parseInt(xy.substring(1, 3));
				
				
				//
				if (xy.length()==4) {
					 xpos = Integer.parseInt(xy.substring(2, 3));
					 sourceXPosition = xpos + "".trim();
					 sourceYPosition = getValofPositionY(xy.substring(0, 2));
				}
				if (xy.length()==3) {
					 xpos = Integer.parseInt(xy.substring(1, 3));
					 sourceXPosition = xpos + "".trim();
					 sourceYPosition = getValofPositionY(xy.substring(0, 1));
				}
				//
				logger.debug(Integer.parseInt(sourceXPosition) + "::" + sourceYPosition);
			}
			
			System.out.println("barcode .." + barcode);
			System.out.println("x .." + sourceXPosition);
			System.out.println("y .." + sourceYPosition);
			
			prepStmt = mosaicConnection.prepareStatement(qry);
			prepStmt.setString(1,barcodeArr[0] );
			prepStmt.setInt(2,Integer.parseInt(sourceXPosition) );
			prepStmt.setInt(3,Integer.parseInt(sourceYPosition) );
			rs = prepStmt.executeQuery();
			if(rs.next()) {
				 
				if (rs.getString("substanceid")!=null && !rs.getString("substanceid").equalsIgnoreCase("")) 
					substanceid = rs.getString("substanceid");
				else
					substanceid = "";
			}
			
			prepStmt.close();
			rs.close();
		
			isSubstanceExists = !substanceid.equalsIgnoreCase("") ? true : false;
			
		} catch (Exception e) {
			e.printStackTrace();
			prepStmt.close();
			rs.close();
		}
		return isSubstanceExists;
		
		
	}
	
	private boolean checkIfCompoundExists(cb4DTO cb4dto) throws Exception{
		
		boolean isCompoundExists = false;
		PreparedStatement prepStmt = null;
		ResultSet rs = null;
		String compoundId="";
		String namepart0 = "";
		String namepart1="";
		
		try {
			String substanceQry = "select compoundid from invreg_compound where namepart0=? and namepart1=?";
			String [] compoundCorpId = null;
			if (cb4dto.getCompoundId()!=null) {
				compoundCorpId = cb4dto.getCompoundId().split("-");
				namepart0=compoundCorpId[0];
				namepart1=compoundCorpId[1];
			}
			prepStmt = mosaicConnection.prepareStatement(substanceQry);
			prepStmt.setString(1, namepart0);
			prepStmt.setString(2, namepart1);
			rs = prepStmt.executeQuery();
			if (rs.next()) {
				compoundId = rs.getString("compoundid");
			}
			logger.debug("compoundId check before submit..." + compoundId);
			prepStmt.close();
			rs.close();
		
			isCompoundExists = !compoundId.equalsIgnoreCase("") ? true : false;
			
		} catch (Exception e) {
			e.printStackTrace();
			prepStmt.close();
			rs.close();
		}
		return isCompoundExists;
		
		
	}
	
private boolean checkIfBarcodeIsActive(String barcode) throws Exception{
	
	boolean isActive = false;
	PreparedStatement prepStmt = null;
	ResultSet rs = null;
	String containerId = "";
	int destroyed = 1;
	try {
		String substanceQry = "select  destroyed from inv_labware_item where  LABWAREBARCODE=?";
		if (barcode!=null && !barcode.equalsIgnoreCase("")) {
			containerId = barcode.split(":")[0];
		}
		prepStmt = mosaicConnection.prepareStatement(substanceQry);
		prepStmt.setString(1, containerId);
		rs = prepStmt.executeQuery();
		if (rs.next()) {
			destroyed = rs.getInt("destroyed");
		} else {
			destroyed = 0;
		}
		
		logger.debug("Barcode : "+ barcode +" check if destryoed and track ..." + destroyed);
		prepStmt.close();
		rs.close();
	
		isActive = destroyed==0 ? true : false;
		
	} catch (Exception e) {
		e.printStackTrace();
		prepStmt.close();
		rs.close();
	}
	return isActive;
	
	
}
	
	
	
	private synchronized void invokeAPIForETL(String env) throws Exception {
		logger.debug("started at : " + watch.time(TimeUnit.SECONDS));
		ETLRun etlRun = new ETLRun();
		int initialCount =0;
		int finalCount =0;
			
		try {
			
			initialCount = getTheQueueCountBeforeBatchProcess ();
		System.out.println("before getting data:");
		ArrayList<cb4DTO> list = (ArrayList<cb4DTO>) fetchModifiedSampleData(platoConnection); 
		System.out.println("List size: " + list.size());
		
		int listSize = 0;
		if (list!=null && list.size() > 2000) {
			listSize = 2000;
		}
		else {
			listSize = list.size();
		}
		System.out.println("final listSize :" + listSize);		
		//for (cb4DTO i : list ) {
		for (int j = 0; j < listSize ; j++) {
			cb4DTO i = (cb4DTO) list.get(j);
			// when operation is disposed
			 if (i.getOperationType().equalsIgnoreCase("DISPOSED")  ) {
				 String disposed = this.disposeLabwareItem(i);
				 logger.debug("Disposed .." + disposed);
			 }
			
			// Transfer
			if (i.getOperationType().equalsIgnoreCase("TRANSFER") ) {
				String transferredDrySample="";
				System.out.println("i.getSubjectContainerId().split(\":\")[0]" + i.getSubjectContainerId().split(":")[0]);
				System.out.println("mosaic types : " + i.getMosaicType() +" ::"+ i.getDestMosaicType());
				String barcode = "";
				String targetBarcode = i.getObjectContainerId();
				if (i.getSubjectContainerId()!=null && !i.getSubjectContainerId().equals("")) {
						barcode = i.getSubjectContainerId().split(":")[0]; 
				}else {
						barcode = i.getSubjectCollectionId();
				}
				
				if (checkIfBarcodeIsActive(barcode)==false || checkIfBarcodeIsActive(barcode)==false )	
					updateQueue("error: Cannot transfer since either of the barcodes has been disposed", i);
				
				
				if (checkIfSubstanceAlreadyExists(targetBarcode) )	
					updateQueue("error: Cannot transfer since SUBSTANCE is already available", i);
				else
					transferredDrySample = this.transferSample(i);					
					logger.debug("transferred .." + transferredDrySample);
			}
			// solvate
			 if (i.getOperationType().equalsIgnoreCase("SOLVATE")  ) {
				 String solvated = "";
				 
					System.out.println("i.getSubjectCollectionId() : " + i.getSubjectCollectionId());
					System.out.println("i.getSubjectContainerId() : " + i.getSubjectContainerId());
				
					 String barcode = "";				 
					 if (i.getSubjectContainerId()!=null && !i.getSubjectContainerId().equals("")) {
							barcode = i.getSubjectContainerId().split(":")[0]; 
						}else {
							barcode = i.getSubjectCollectionId();
						}
				 
				 if (checkIfBarcodeIsActive(barcode)==false )	
						updateQueue("error: Cannot solvate since the barcodes has been disposed", i);
				 else
				 solvated = this.solvateSample(i);
				 logger.debug("SOLVATED .." + solvated);
			 }
			 //submit
			 if (i.getOperationType().equalsIgnoreCase("SUBMIT") ) {
				 String submitted = "";
				 if (checkIfBarcodeIsActive(i.getSubjectContainerId())==false )	
						updateQueue("error: Cannot submit since the barcodes has been disposed", i);
				 else
					 submitted = this.submit(i);
				 	logger.debug("SUBMITTED .." + submitted);
			 }
			 //tareweight
			 if (i.getOperationType().equalsIgnoreCase("TARE")   ) {
				 String tare = "";
				 if (checkIfBarcodeIsActive(i.getSubjectContainerId())==false )	
						updateQueue("error: Cannot tare since the barcodes has been disposed", i);
				 else
					 tare = this.tare(i);
				 	logger.debug("TAREWEIGHT .." + tare);
			 }
			 //final weigh
			 if (i.getOperationType().equalsIgnoreCase("FINALWEIGH")) {
				 String finalWeigh = "";
				 
				 if (checkIfBarcodeIsActive(i.getSubjectContainerId())==false )	
						updateQueue("error: Cannot final weigh since the barcodes has been disposed", i);
				 else				 
					 finalWeigh = this.finalWeigh(i);
				 	logger.debug("Final Weigh ..completed .." + finalWeigh);
			 }
			// dilute : this actually calls solvate
			 if (i.getOperationType().equalsIgnoreCase("DILUTE")  ) {
				 
				 String dilute = "";
				 if (checkIfBarcodeIsActive(i.getSubjectContainerId())==false )	
						updateQueue("error: Cannot dilute since the barcodes has been disposed", i);
				 else
					 dilute = this.solvateSample(i);
				 	logger.debug("Diluted .." + dilute);
			 }
			
			 if (i.getOperationType().equalsIgnoreCase("ADDTOSET") ) {
				
				 String addToSet = "";
				 if (checkIfBarcodeIsActive(i.getSubjectContainerId())==false )	
						updateQueue("error: Cannot add to set since the barcodes has been disposed", i);
				 else 
					 addToSet = this.addToSet(i);
				 	 logger.debug("addToSet .." + addToSet);
			 }
			 
			if ( i.getOperationType().equalsIgnoreCase("UPDATE") ) {
				etlRun.retryConnections();
				
				if (checkIfBarcodeIsActive(i.getSubjectContainerId())==false )	
					updateQueue("error: Cannot update to set since the barcodes has been disposed", i);
				else {
				
					PlatoContainer platoContainer = etlRun.getPlatContainerDetails(i);
					logger.debug("Updated .." + platoContainer.getContainerId()  + " at location : " + platoContainer.getLocationId()	 +"platoContainer.getContainerSid() = " + platoContainer.getContainerSid());
					if (etlRun.checkIfContainerIdExistsForUpdate(i.getOperationSid(),platoConnection ) )	{			
						etlRun.incTransferCB4PlatoContainerSamplesToMosaic("" ,platoContainer.getContainerSid(), 
							platoContainer.getContainerId(),"	",null,i.getActionLocation(), 0,"");
						updateQueueForUpdate(i.getOperationSid(), "Successfully Updated : " + i.getSubjectContainerId() );
					  
					} else {
						updateQueueForUpdate(i.getOperationSid(),"Invalid subject/object container_id");
					}
				}
			}
			
			if (i.getOperationType().equalsIgnoreCase("MOVE") ) {
				etlRun.retryConnections();
				
				PlatoContainer platoContainer = etlRun.getPlatContainerDetails(i);
				String barcode="";
				boolean isCollection = false;
				if (platoContainer.getContainerId()!=null) {
					barcode=platoContainer.getContainerId();
					isCollection = false;
				}
				else {
					barcode=i.getSubjectCollectionId();
					isCollection = true;
				}
				
				if (checkIfBarcodeIsActive(barcode)==false )	
					updateQueue("error: Cannot update to set since the barcodes has been disposed", i);
				else {
									
					logger.debug("Updated .." + platoContainer.getContainerId()  + " at location : " + platoContainer.getLocationId()	 +"platoContainer.getContainerSid() = " + platoContainer.getContainerSid());
					if (this.checkIfLabwareExistsForMove(barcode ) )	{	
						try {
//							System.out.println("barcode: " + barcode);
//							System.out.println("platoContainer.getContainerSid(): " + platoContainer.getContainerSid());
//							System.out.println("i.getActionLocation(): " + i.getActionLocation());
//							System.out.println("isCollection: " + isCollection);
							etlRun.incTransferCB4PlatoContainerSamplesToMosaicForMove("" ,platoContainer.getContainerSid(), 
									barcode,"	",null,i.getActionLocation(), 0,"",isCollection);
							updateQueueForUpdate(i.getOperationSid(), "Successfully MOVED : " + barcode );
						} catch (Exception e) {
							updateQueueForUpdate(i.getOperationSid(),"Invalid subject/object container_sid for MOVE Operation :: " + e.getMessage());
						}	
					}else {
						updateQueueForUpdate(i.getOperationSid(),"No LabwareItem is avaiable for this container / collection for MOVE Operation :: " + barcode);
					}
				}

			}
			
			if (i.getOperationType().equalsIgnoreCase("DISPATCH")  ) {
				System.out.println("i.getSubjectCollectionId() : " + i.getSubjectCollectionId());
				System.out.println("i.getSubjectContainerId() : " + i.getSubjectContainerId());
				
				 int dispatch = 0;
				 String barcode = "";
		 	
				 
				 if (i.getSubjectContainerId()!=null && !i.getSubjectContainerId().equals("")) {
						barcode = i.getSubjectContainerId().split(":")[0]; 
					}else {
						barcode = i.getSubjectCollectionId();
					}
					
					//prepStmt.setString(1, cb4dto.getSubjectContainerId().split(":")[0]);
					System.out.println("Container id / Collection id  : " + barcode );
					
				 
				 	if (checkIfBarcodeIsActive(barcode )==false )	
						updateQueue("error: Cannot dispatch since the barcodes has been disposed", i);
				 else 
					 dispatch = this.setDispatch(barcode);
				 updateQueueForUpdate(i.getOperationSid(), "Successfully Dispatched : " + barcode );
				 	 logger.debug("DISPATCHED .." + dispatch); 	 
			 }
			
			
			
			// solventWETSample : this actually calls solvate wet sample
			 if (i.getOperationType().equalsIgnoreCase("ADDSOLVENT")  ) {
				 
				 String barcode = "";				 
				 if (i.getSubjectContainerId()!=null && !i.getSubjectContainerId().equals("")) {
						barcode = i.getSubjectContainerId().split(":")[0]; 
					}else {
						barcode = i.getSubjectCollectionId();
					}
				 
				 System.out.println(" i.getCompoundId() : " + i.getCompoundId());
				 System.out.println(" i.getSolvent() : " + i.getSolvent());
				 System.out.println(" i.getSolventAmount() : " + i.getSolventAmount());
				 System.out.println(" i.getSolventUnits() : " + i.getSolventUnits() );
				 String solventWETSample = "";
				 if (checkIfBarcodeIsActive(barcode)==false )	
						updateQueue("error: Cannot be solvated this wet sample, since the barcodes has been disposed", i);
				 else if (i.getCompoundId()==null && (i.getSolvent()!=null && !(i.getSolvent()).equals("") ) ) {
					 solventWETSample = this.solventWETSample(i);
					 System.out.println("solventWETSample .." + solventWETSample);
				 } else {
					 updateQueue("error: Cannot be solvated this wet sample, since it has compound and / or solvent text is null", i);
				 }
					
			 }
			 
		}
		
		finalCount = getTheQueueCountBeforeBatchProcess ();
		etlRun.closeAllConnections();
		
		platoConnection.close();
		mosaicConnection.close();
			
//		Properties prop = ConfigService.readProperties();
//		emailUtils = new EmailUtils();
//		emailUtils.initialize("cb4", prop);	
//		StringBuffer emailBody = new StringBuffer();
//		emailBody = emailBody.append("Count verification for this batch job...\n\n");
//		emailBody.append("initalCount = ");
//		emailBody.append(initialCount);
//		emailBody.append("\n\nfinal count = ");
//		emailBody.append(finalCount);
//		emailBody.append("\n\nNo.of Records Processed= ");
//		emailBody.append(initialCount-finalCount);
//		
//		emailUtils.sendErrorMessage( "SYSENG " + env + ":" + "ETL Job count Statistics",	emailBody+"" );
		
		//emailUtils.sendErrorMessage( "SYSENG TEST" + ":" + "ETL Job count Statistics",		emailBody+"" );
		
		logger.debug("Finished at : " + watch.time (TimeUnit.SECONDS));
		
		} catch (Exception e) {
			Properties prop = ConfigService.readProperties();
			emailUtils = new EmailUtils();
			emailUtils.initialize("cb4", prop);	
			emailUtils.sendErrorMessage("SYSENG " + env +" : Exception / Error in SYSENG PROD ETL batch process", e.getMessage() );
			//emailUtils.sendErrorMessage("SYSENG TEST : Exception / Error in SYSENG TEST ETL batch process", e.getMessage() );
		}
	}

	
	
	
	public void getAddress() { 
		PreparedStatement prepStmt = null;
		ResultSet rs = null;
		PreparedStatement prepStmt1 = null;
		String qry = 
				
//				"select company, last_name, first_name, full_name, address, phone_no, email_id,"
//				+ " country, country_abbr from plato.v_mta_contact";
		" select company, recipient, address, phone_no, email_addr, country from  zorro.tmp_grx_prod_address ";
		
		String insertAddress = "INSERT INTO temp_add2 ( id, address1, address2, city, statecode, postalcode,"
				+ "company,RECIPIENT,EMAILADDRESS,ACTIVE,phone,country,country_abbr ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";
		String address = "";
		int i=1;
		String address1="";
		String address2="";
		String city="";
		String statecode="";
		String postalcode="";
		String company="";
		String recipent="";
		String email="";
		int active=1;
		String phone="";
		String country="";
		String country_abbr="";
		
		try {
			 prepStmt1 = platoConnection.prepareStatement(insertAddress);
			prepStmt = platoConnection.prepareStatement(qry);
			rs = prepStmt.executeQuery();
			rs.setFetchSize(10);
			
			while(rs.next()) {
				company = rs.getString("company");
				recipent=rs.getString("recipient");
				email=rs.getString("email_addr");
				 phone=rs.getString("phone_no");
				 country=rs.getString("country");
				 country_abbr="";
				i++;
				//logger.debug("----------------------------");
				address = rs.getString("address");
				String [] add = address.split("\n");
				
				logger.debug("add length : " + add.length);
				
				if (add.length==1) {
					logger.debug("Address length 1 " + add);
					if (add[0].split(" ").length==2) {
						logger.debug("Address 0" + add[0]);
						
						 address1=add[0].split(" ")[0]!=null ? add[0].split(" ")[0] : "";
						 address2="";
						 city="";
						 statecode="";
						 postalcode=add[0].split(" ")[1]!=null ? add[0].split(" ")[1] : "";
					 }
					 					
					if (add[0].split(" ").length==3) {
						
						 address1=add[0].split(" ")[0]!=null ? add[0].split(" ")[0] : "";
						 address2="";
						 city="";
						 statecode=add[0].split(" ")[1]!=null ? add[0].split(" ")[1] : "";
						 postalcode=add[0].split(" ")[2]!=null ? add[0].split(" ")[2] : "";
					 }
					
					if (add[0].split(" ").length==4) {
						
						 address1=add[0].split(" ")[0]!=null ? add[0].split(" ")[0] : "";
						 address2="";
						 city=add[0].split(" ")[1]!=null ? add[0].split(" ")[1] : "";
						 statecode=add[0].split(" ")[2]!=null ? add[0].split(" ")[2] : "";
						 postalcode=add[0].split(" ")[3]!=null ? add[0].split(" ")[3] : "";
					 }
					
					 if (add[0].split(" ").length==5) {
							
						 address1=add[0].split(" ")[0]!=null ? add[0].split(" ")[0] : "";
						 address2=add[0].split(" ")[1]!=null ? add[0].split(" ")[1] : "";
						 city=add[0].split(" ")[2]!=null ? add[0].split(" ")[2] : "";
						 statecode=add[0].split(" ")[3]!=null ? add[0].split(" ")[3] : "";
						 postalcode=add[0].split(" ")[4]!=null ? add[0].split(" ")[4] : "";
					 }
					
					 if (add[0].split(" ").length==6) {
							
						 address1=add[0].split(" ")[0] +" " + add[0].split(" ")[1];
						 address2=add[0].split(" ")[2]!=null ? add[0].split(" ")[2] : "";
						 city=add[0].split(" ")[3]!=null ? add[0].split(" ")[3] : "";
						 statecode=add[0].split(" ")[4]!=null ? add[0].split(" ")[4] : "";
						 postalcode=add[0].split(" ")[5]!=null ? add[0].split(" ")[5] : "";
					 }
					 
					 if (add[0].split(" ").length==7) {
							
						 address1=add[0].split(" ")[0] +" " + add[0].split(" ")[1] +" " + add[0].split(" ")[2];
						 address2=add[0].split(" ")[3]!=null ? add[0].split(" ")[3] : "";
						 city=add[0].split(" ")[4]!=null ? add[0].split(" ")[4] : "";
						 statecode=add[0].split(" ")[5]!=null ? add[0].split(" ")[5] : "";
						 postalcode=add[0].split(" ")[6]!=null ? add[0].split(" ")[6] : "";
					 }
					 if (add[0].split(" ").length==8) {
							
						 address1=add[0].split(" ")[0] +" " + add[0].split(" ")[1] +" " + add[0].split(" ")[2]+" " + add[0].split(" ")[3];
						 address2=add[0].split(" ")[4]!=null ? add[0].split(" ")[4] : "";
						 city=add[0].split(" ")[5]!=null ? add[0].split(" ")[5] : "";
						 statecode=add[0].split(" ")[6]!=null ? add[0].split(" ")[6] : "";
						 postalcode=add[0].split(" ")[7]!=null ? add[0].split(" ")[7] : "";
					 }
					 if (add[0].split(" ").length==9) {
							
						 address1=add[0].split(" ")[0] +" " + add[0].split(" ")[1] +" " + add[0].split(" ")[2]+" " + add[0].split(" ")[3]+" " + add[0].split(" ")[4];
						 address2=add[0].split(" ")[5]!=null ? add[0].split(" ")[5] : "";
						 city=add[0].split(" ")[6]!=null ? add[0].split(" ")[6] : "";
						 statecode=add[0].split(" ")[7]!=null ? add[0].split(" ")[7] : "";
						 postalcode=add[0].split(" ")[8]!=null ? add[0].split(" ")[8] : "";
					 }
					 if (add[0].split(" ").length==10) {
							
						 address1=add[0].split(" ")[0] +" " + add[0].split(" ")[1] +" " + add[0].split(" ")[2]+" " + add[0].split(" ")[3]+" " + add[0].split(" ")[4]+" " + add[0].split(" ")[5];
						 address2=add[0].split(" ")[6]!=null ? add[0].split(" ")[6] : "";
						 city=add[0].split(" ")[7]!=null ? add[0].split(" ")[7] : "";
						 statecode=add[0].split(" ")[8]!=null ? add[0].split(" ")[8] : "";
						 postalcode=add[0].split(" ")[9]!=null ? add[0].split(" ")[9] : "";
					 }
					 if (add[0].split(" ").length==11) {
							
						 address1=add[0].split(" ")[0] +" " + add[0].split(" ")[1] +" " + add[0].split(" ")[2]+" " + add[0].split(" ")[3]+" " + add[0].split(" ")[4]+" " + add[0].split(" ")[5]+" " + add[0].split(" ")[6];
						 address2=add[0].split(" ")[7]!=null ? add[0].split(" ")[7] : "";
						 city=add[0].split(" ")[8]!=null ? add[0].split(" ")[8] : "";
						 statecode=add[0].split(" ")[9]!=null ? add[0].split(" ")[9] : "";
						 postalcode=add[0].split(" ")[10]!=null ? add[0].split(" ")[10] : "";
					 }
					 if (add[0].split(" ").length==12) {
							
						 address1=add[0].split(" ")[0] +" " + add[0].split(" ")[1] +" " + add[0].split(" ")[2]+" " + add[0].split(" ")[3]+" " + add[0].split(" ")[4]+" " + add[0].split(" ")[5]+" " + add[0].split(" ")[6]+" " + add[0].split(" ")[7];
						 address2=add[0].split(" ")[8]!=null ? add[0].split(" ")[8] : "";
						 city=add[0].split(" ")[9]!=null ? add[0].split(" ")[9] : "";
						 statecode=add[0].split(" ")[10]!=null ? add[0].split(" ")[10] : "";
						 postalcode=add[0].split(" ")[11]!=null ? add[0].split(" ")[11] : "";
					 }
					 if (add[0].split(" ").length==13) {
							
						 address1=add[0].split(" ")[0] +" " + add[0].split(" ")[1] +" " + add[0].split(" ")[2]+" " 
								 + add[0].split(" ")[3]+" " + add[0].split(" ")[4]+" " + add[0].split(" ")[5]+" " 
								 + add[0].split(" ")[6]+" " + add[0].split(" ")[7]+" " + add[0].split(" ")[8];
						 address2=add[0].split(" ")[9]!=null ? add[0].split(" ")[9] : "";
						 city=add[0].split(" ")[10]!=null ? add[0].split(" ")[10] : "";
						 statecode=add[0].split(" ")[11]!=null ? add[0].split(" ")[11] : "";
						 postalcode=add[0].split(" ")[12]!=null ? add[0].split(" ")[12] : "";
					 }
					 if (add[0].split(" ").length==14) {
							
						 address1=add[0].split(" ")[0] +" " + add[0].split(" ")[1] +" " + add[0].split(" ")[2]+" " 
								 + add[0].split(" ")[3]+" " + add[0].split(" ")[4]+" " + add[0].split(" ")[5]+" " 
								 + add[0].split(" ")[6]+" " + add[0].split(" ")[7]+" " + add[0].split(" ")[8] + add[0].split(" ")[9];
						 address2=add[0].split(" ")[10]!=null ? add[0].split(" ")[10] : "";
						 city=add[0].split(" ")[11]!=null ? add[0].split(" ")[11] : "";
						 statecode=add[0].split(" ")[12]!=null ? add[0].split(" ")[12] : "";
						 postalcode=add[0].split(" ")[13]!=null ? add[0].split(" ")[13] : "";
					 }
					logger.debug(address1 + " : "+address2 +" : "+city + " : " +statecode+" : "+postalcode);
				}
				
				if (add.length==2) {
					 address1=add[0];
					 address2="";
					 logger.debug("Address length 2 " + add);
					 if (add[1].split(" ").length==2) {
						 city="";
						 statecode=add[1].split(" ")[0]!=null ? add[1].split(" ")[0] : "";
						 postalcode=add[1].split(" ")[1]!=null ? add[1].split(" ")[1] : "";
						// postalcode=add[2].split(" ")[2]!=null ? add[2].split(" ")[2] : "";
					 }if (add[1].split(" ").length ==3) {
						 city=add[1].split(" ")[0]!=null ? add[1].split(" ")[0] : "";
						 statecode=add[1].split(" ")[1]!=null ? add[1].split(" ")[1] : "";
						 postalcode=add[1].split(" ")[2]!=null ? add[1].split(" ")[2] : "";
					 }
					
					logger.debug(address1 + " : "+address2 +" : "+city + " : " +statecode+" : "+postalcode);
				}
				
				if (add.length==3) {
					 address1=add[0];
					 address2=add[1];
					
					 if (add[2].split(" ").length==2) {
						 city="";
						 statecode=add[2].split(" ")[0]!=null ? add[2].split(" ")[0] : "";
						 postalcode=add[2].split(" ")[1]!=null ? add[2].split(" ")[1] : "";
						// postalcode=add[2].split(" ")[2]!=null ? add[2].split(" ")[2] : "";
					 }if (add[2].split(" ").length ==3) {
						 city=add[2].split(" ")[0]!=null ? add[2].split(" ")[0] : "";
						 statecode=add[2].split(" ")[1]!=null ? add[2].split(" ")[1] : "";
						 postalcode=add[2].split(" ")[2]!=null ? add[2].split(" ")[2] : "";
					 }
					
					logger.debug(address1 + " : "+address2 +" : "+city + " : " +statecode+" : "+postalcode);
				}
				
				if (add.length==4) {
					 address1=add[0];
					 address2=add[1];
					 city = add[2];
					
					 if (add[3].split(" ").length==2) {
						 statecode=add[3].split(" ")[0]!=null ? add[3].split(" ")[0] : "";
						 postalcode=add[3].split(" ")[1]!=null ? add[3].split(" ")[1] : "";
					 }if (add[3].split(" ").length ==3) {
						 city=add[3].split(" ")[0]!=null ? add[3].split(" ")[0] : "";
						 statecode=add[3].split(" ")[1]!=null ? add[3].split(" ")[1] : "";
						 postalcode=add[3].split(" ")[2]!=null ? add[3].split(" ")[2] : "";
					 }
					
					logger.debug(address1 + " : "+address2 +" : "+city + " : " +statecode+" : "+postalcode);
				}
				
				if (add.length==5) {
					 address1=add[0];
					 address2=add[1] +" " +add[2];
					
					 if (add[3].split(" ").length==2) {
						 city="";
						 statecode=add[3].split(" ")[0]!=null ? add[3].split(" ")[0] : "";
						 postalcode=add[3].split(" ")[1]!=null ? add[3].split(" ")[1] : "";
					 }if (add[3].split(" ").length ==3) {
						 city=add[3].split(" ")[0]!=null ? add[3].split(" ")[0] : "";
						 statecode=add[3].split(" ")[1]!=null ? add[3].split(" ")[1] : "";
						 postalcode=add[3].split(" ")[2]!=null ? add[3].split(" ")[2] : "";
					 }
					
					logger.debug(address1 + " : "+address2 +" : "+city + " : " +statecode+" : "+postalcode);
				}
				
				if (add.length==6) {
					 address1=add[0];
					 address2=add[1]+ " " +add[2];
					 city=add[4];
					 if (add[5].split(" ").length>1) {
					 statecode=add[5].split(" ")[0];
					 postalcode=add[5].split(" ")[1];
					 }else {
						 statecode="";
						 postalcode=add[5].split(" ")[0];
					 }
					
					logger.debug(address1 + " : "+address2 +" : "+city + " : " +statecode+" : "+postalcode);
				}
				
				
				
				for (int j = 0; j < add.length; j++) {
					logger.debug("j: "+ j + " : " + add[j] + "\t");
				}
				
			  logger.debug("----------------------------record : "+ i);
			  
			  statecode=statecode.length()>2 ? statecode.trim().substring(0,1) : statecode;
			 
			  prepStmt1.setInt(1, i);
			  prepStmt1.setString(2, address1);
			  prepStmt1.setString(3, address2);
			  prepStmt1.setString(4, city);
			  prepStmt1.setString(5, statecode);
			  prepStmt1.setString(6, postalcode);
			  prepStmt1.setString(7, company);
			  prepStmt1.setString(8, recipent);
			  prepStmt1.setString(9, email);
			  prepStmt1.setInt(10, active);
			  prepStmt1.setString(11,phone);
			  prepStmt1.setString(12,country);
			  prepStmt1.setString(13,country_abbr);
			  prepStmt1.executeUpdate();
			  
			}
			
		} catch (SQLException e) {
			
			e.printStackTrace();
		} finally {
			
		}
		
	}
	
	public void setAddress2() {
		String qry = "select * from temp_address where address2 is null";
		String updateQry = "update temp_address set address1=?, address2=? where id=? ";
		PreparedStatement prepStmt = null;
		PreparedStatement prepUpDateStmt = null;
		ResultSet rs = null;
		String add2 =null;
		String add0=null;
		try {
			
			prepStmt = platoConnection.prepareStatement(qry);
			rs = prepStmt.executeQuery();
			prepUpDateStmt = platoConnection.prepareStatement(updateQry);
			while(rs.next()) {
				logger.debug("-----------------------");
				int id= rs.getInt(1);
				String add1 = rs.getString(2);
				if (add1.split(",").length ==2) {
					add0=add1.split(",")[0];
					add2 = add1.split(",")[1];			
					prepUpDateStmt.setString(1, add0);
					prepUpDateStmt.setString(2, add2);
					prepUpDateStmt.setInt(3, id);
					prepUpDateStmt.executeUpdate();
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}
	
	
	
	
	/*
	 * these are for SUBMIT.....................
	 */
	
	private synchronized String setNeatSample(cb4DTO cb4dto) throws Exception {

		logger.debug("setNeatSample starts here....................");
		PreparedStatement prepStmt = null;
		ResultSet rs = null;
		String sourceLabwareItemId = "";
		String sourceSubstanceId = "";
		String jsonResult = "";
		String sourceXPosition = "";
		String sourceYPosition = "";
		String barcode="";
		String namepart0="";
		String namepart1="";
		String url = this.mosaicAPIUrl;
		
		try {
			String substanceQry = "select compoundid from invreg_compound where namepart0=? and namepart1=?";
			String [] compoundCorpId = null;
			if (cb4dto.getCompoundId()!=null) {
				compoundCorpId = cb4dto.getCompoundId().split("-");
				namepart0=compoundCorpId[0];
				namepart1=compoundCorpId[1];
			}
			prepStmt = mosaicConnection.prepareStatement(substanceQry);
			prepStmt.setString(1, namepart0);
			prepStmt.setString(2, namepart1);
			rs = prepStmt.executeQuery();
			if (rs.next()) {
				sourceSubstanceId = rs.getString("compoundid");
			}
			logger.debug("setNeatSample sourceSubstanceId..." + sourceSubstanceId);
			prepStmt.close();
			rs.close();
			
			
			prepStmt = mosaicConnection.prepareStatement(FETCH_LABWAREITEMID_FOR_CONTAINER_BARCODE);
			prepStmt.setString(1, cb4dto.getSubjectContainerId());
			rs = prepStmt.executeQuery();
			if (rs.next()) {
				sourceLabwareItemId = rs.getString("LABWAREITEMID");
				//sourceSubstanceId = rs.getString("SUBSTANCEID");
			}
			logger.debug("setNeatSample labwareItemId..." + sourceLabwareItemId);
			prepStmt.close();
			rs.close();
			
			//jsonResult = this.getSampleHolders("testB0243190", "testType");
			jsonResult = this.getSampleHolders(cb4dto.getSubjectContainerId().split(":")[0], cb4dto.getMosaicType());
					
			logger.debug("setNeatSample-- sample holder details are available: " + jsonResult);
			jsonResult = jsonResult.substring(1, jsonResult.length() - 1);
			JSONObject obj = new JSONObject(jsonResult.toString());
			JSONArray sampleHolders = obj.getJSONArray("sampleHolders");
			JSONObject sampleHolderIdentifier = new JSONObject(
					sampleHolders.getJSONObject(0).get("sampleHolderIdentifier").toString());
			JSONObject position = new JSONObject(sampleHolderIdentifier.get("position").toString());
			
			if (sourceLabwareItemId == null || sourceLabwareItemId.equals("")) {
					sourceLabwareItemId = sampleHolderIdentifier.get("labwareItemId").toString();
			}
			sourceXPosition = position.get("x").toString();
			sourceYPosition = position.get("y").toString();
			// logger.debug("sourceXPosition = " + sourceXPosition + " sourceYPosition
			// = " + sourceYPosition);
			url = url + "/SampleHolders/"+sourceLabwareItemId+"@("+sourceXPosition+","+sourceYPosition+")/SetNeatSample";

			logger.debug("setNeatSample api..." + url);
			logger.debug("setNeatSample : " + cb4dto.getSourceTareWt());

			StringEntity params = new StringEntity("{" + 
					"  \"neatSample\": {" + 
					"    \"constituents\": [" + 
					"      {" + 
					"        \"substanceId\": "+sourceSubstanceId+"," + 
					"        \"amount\": {" + 
					"          \"value\": "+cb4dto.getSubjectContainerNetwt()+"" + 
					"        }" + 
					"      }" + 
					"    ]" + 
					"  }" + 
					"}");
			logger.debug("params :" + params);
			jsonResult = getAPIResponse(url, "POST", params);
			logger.debug("setNeatSample output done ");
			logger.debug("setNeatSample ..........jsonResult=" + jsonResult);
			updateQueue(jsonResult, cb4dto);
			return jsonResult;
		} catch (Exception e) {
			if (rs != null && !rs.isClosed())
				rs.close();
			if (prepStmt != null && !prepStmt.isClosed())
				prepStmt.close();
			//throw e;
			logger.debug("------------------------tracking the error.............." + e.getMessage());
			updateQueue(e.getMessage(), cb4dto);
			return e.getMessage();
		} finally {

		}
	}
	
	
	private synchronized String setSolutionSample(cb4DTO cb4dto) throws Exception {

		logger.debug("setSolutionSample starts here....................");
		PreparedStatement prepStmt = null;
		ResultSet rs = null;
		String sourceLabwareItemId = "";
		String sourceSubstanceId = "";
		String jsonResult = "";
		String sourceXPosition = "";
		String sourceYPosition = "";
		String barcode="";
		String namepart0="";
		String namepart1="";
		String url = this.mosaicAPIUrl;
		
		try {
			String substanceQry = "select compoundid from invreg_compound where namepart0=? and namepart1=?";
			String [] compoundCorpId = null;
			if (cb4dto.getCompoundId()!=null) {
				compoundCorpId = cb4dto.getCompoundId().split("-");
				namepart0=compoundCorpId[0];
				namepart1=compoundCorpId[1];
			}
			prepStmt = mosaicConnection.prepareStatement(substanceQry);
			prepStmt.setString(1, namepart0);
			prepStmt.setString(2, namepart1);
			rs = prepStmt.executeQuery();
			if (rs.next()) {
				sourceSubstanceId = rs.getString("compoundid");
			}
			logger.debug("setSolutionSample labwareItemId..." + sourceSubstanceId);
			prepStmt.close();
			rs.close();
			
			
			prepStmt = mosaicConnection.prepareStatement(FETCH_LABWAREITEMID_FOR_CONTAINER_BARCODE);
			prepStmt.setString(1, cb4dto.getSubjectContainerId());
			rs = prepStmt.executeQuery();
			if (rs.next()) {
				sourceLabwareItemId = rs.getString("LABWAREITEMID");
				//sourceSubstanceId = rs.getString("SUBSTANCEID");
			}
			logger.debug("setSolutionSample labwareItemId..." + sourceLabwareItemId);
			prepStmt.close();
			rs.close();
			
			//jsonResult = this.getSampleHolders("testB0243190", "testType");
			jsonResult = this.getSampleHolders(cb4dto.getSubjectContainerId().split(":")[0], cb4dto.getMosaicType());
					
			logger.debug("setSolutionSample-- sample holder details are available: " + jsonResult);
			jsonResult = jsonResult.substring(1, jsonResult.length() - 1);
			JSONObject obj = new JSONObject(jsonResult.toString());
			JSONArray sampleHolders = obj.getJSONArray("sampleHolders");
			JSONObject sampleHolderIdentifier = new JSONObject(
					sampleHolders.getJSONObject(0).get("sampleHolderIdentifier").toString());
			JSONObject position = new JSONObject(sampleHolderIdentifier.get("position").toString());
			
			if (sourceLabwareItemId == null || sourceLabwareItemId.equals("")) {
				sourceLabwareItemId = sampleHolderIdentifier.get("labwareItemId").toString();
			}
			
			sourceXPosition = position.get("x").toString();
			sourceYPosition = position.get("y").toString();
			// logger.debug("sourceXPosition = " + sourceXPosition + " sourceYPosition
			// = " + sourceYPosition);
			url = url + "/SampleHolders/"+sourceLabwareItemId+"@("+sourceXPosition+","+sourceYPosition+")/SetSolutionSample";
			
			logger.debug("setSolutionSample api..." + url);
			logger.debug("setSolutionSample : " + cb4dto.getSubjectContainerVolume());
			
			
			int concentrationUnitType = 0;
			int concentrationDisplayExponent = 0;

			if (cb4dto.getSubjectContainerConcUnits() != null
					&& cb4dto.getSubjectContainerConcUnits().equalsIgnoreCase("uM")) {

				concentrationUnitType = 1;
				concentrationDisplayExponent = -3;
			}
			if (cb4dto.getSubjectContainerConcUnits() != null
					&& cb4dto.getSubjectContainerConcUnits().equalsIgnoreCase("mM")) {

				concentrationUnitType = 1;
				concentrationDisplayExponent = -3;
			}
			
			Double d = 0.0;
			int resolution = 0;
			if (cb4dto.getSubjectContainerVolume()!=null  && !cb4dto.getSubjectContainerVolume().equalsIgnoreCase("")) {
				d = Double.parseDouble(cb4dto.getSubjectContainerVolume());
			}
			BigDecimal b = BigDecimal.valueOf(d);		
			
			if ( b.scale()>=6 ) {
				String str = String.valueOf(d);
				String [] arrStr = str.split("\\.") ;
				String val = arrStr[1].substring(0, 5);
				logger.debug(val);
				String finalStrVal = arrStr[0]+"."+arrStr[1];
				d= Double.parseDouble(finalStrVal);
				logger.debug("final set value to 6 decimails" + d );
				logger.debug("greater than or equal to 6 so setting to 6 only : " + b.scale());
				resolution=-6;
				
			}else {
				logger.debug("less than than 6 so setting to same scale : "  + b.scale());
				resolution = b.scale()*-1;
			}
			
			logger.debug("resolution ... " + resolution); 
			logger.debug("final double value ... " + d); 
			
			StringEntity params = new StringEntity("{" + 
					"   \"solutionSample\": {" + 
					"    \"solvents\": [" + 
					"        {\"solventId\": 0," + 
					"        \"proportion\": 1}" + 
					"    ]," + 
					"    \"constituents\": [" + 
					"      {" + 
					"        \"substanceId\": "+ sourceSubstanceId +" ," + 
					"        \"concentration\": {" + 
					"          \"unitType\": "+ concentrationUnitType +" ," + 
					"          \"value\": "+ cb4dto.getSubjectContainerConc() +"," + 
					"          \"displayExponent\": " + concentrationDisplayExponent + "," + 
					"           \"resolution\": "+ resolution+"   " + 
					"        }" + 
					"      }" + 
					"    ]," + 
					"    \"volume\": {" + 
					"          \"value\": "+ cb4dto.getSubjectContainerVolume() +"," + 
					"          \"displayExponent\": " + concentrationDisplayExponent + "," + 
					"           \"resolution\": "+ resolution+"   " + 
					"        }" + 
					"   }" +  
					"}"); 
			jsonResult = getAPIResponse(url, "POST", params);
			logger.debug("setSolutionSample output done ");
			logger.debug("setSolutionSample ..........jsonResult=" + jsonResult);
			updateQueue(jsonResult, cb4dto);
			return jsonResult;
		} catch (Exception e) {
			if (rs != null && !rs.isClosed())
				rs.close();
			if (prepStmt != null && !prepStmt.isClosed())
				prepStmt.close();
			//throw e;
			logger.debug("----setSolutionSample--------------------tracking the error.............." + e.getMessage());
			updateQueue(e.getMessage(), cb4dto);
			return e.getMessage();
		} finally {

		}
	}
	
	
	private int setDispatch(String barcode) throws Exception{
		
		PreparedStatement prepStmt = null;
		String containerId = "";
		int despatched = 0;
		try {
			String substanceQry = "update inv_labware_item set despatched=1 where labwarebarcode = ?";
			
			if (barcode!=null && !barcode.equalsIgnoreCase("")) {
				containerId = barcode.split(":")[0];
			}
			prepStmt = mosaicConnection.prepareStatement(substanceQry);
			prepStmt.setString(1, containerId);
			despatched = prepStmt.executeUpdate();
			logger.debug("Barcode : "+ barcode +" has been despatched ..." + despatched);
			prepStmt.close();		
			
		} catch (Exception e) {
			prepStmt.close();
		}
		return despatched;
	}
	
	// sovlate wet sample.
	
	/*
	curl --location --request POST 'https://takeda.mosaic-cloud.com/api/Inventory/SampleHolders/4505330@(24,14)/AddSolventSample' \
	--header 'Authorization: Basic TW9zYWljQVBJVXNlcjpNMHNhaWNBUElAMTAw' \
	--header 'Content-Type: application/json' \
	--data-raw '{
	   "solvents": [
	        {"solventId": 0,
	        "proportion": 1}
	    ],
	    "volume": {
	          "value": 2,
	           "displayExponent":-3,
	           "resolution":2
	        }
	   
	}

	'	
	*/
	
	
		private synchronized String solventWETSample(cb4DTO cb4dto) throws Exception {

			logger.debug("solvate starts here....................");
			PreparedStatement prepStmt = null;
			ResultSet rs = null;
			String sourceLabwareItemId = "";
			String jsonResult = "";
			String sourceXPosition = "";
			String sourceYPosition = "";
			String url = this.mosaicAPIUrl; //"https://takeda.mosaic-cloud.com/api/Inventory";

			try {
				prepStmt = mosaicConnection.prepareStatement(FETCH_LABWAREITEMID_FOR_CONTAINER_BARCODE);
				prepStmt.setString(1, cb4dto.getSubjectContainerId());
				rs = prepStmt.executeQuery();
				if (rs.next()) {
					sourceLabwareItemId = rs.getString("LABWAREITEMID");
				}
				logger.debug("source labwareItemId..." + sourceLabwareItemId);
				prepStmt.close();
				rs.close();
				jsonResult = this.getSampleHolders(cb4dto.getSubjectContainerId().split(":")[0], cb4dto.getMosaicType());
				logger.debug("------------->>>>>" + jsonResult);
				
				jsonResult = jsonResult.substring(1, jsonResult.length() - 1);
				JSONObject obj = new JSONObject(jsonResult.toString());
				JSONArray sampleHolders = obj.getJSONArray("sampleHolders");
				logger.debug(sampleHolders);
				logger.debug("------------------");
				JSONObject sampleHolderIdentifier = new JSONObject(
						sampleHolders.getJSONObject(0).get("sampleHolderIdentifier").toString());
				JSONObject position = new JSONObject(sampleHolderIdentifier.get("position").toString());
				if (sourceLabwareItemId == null || sourceLabwareItemId.equals("")) {
					sourceLabwareItemId = sampleHolderIdentifier.get("labwareItemId").toString();
				}
				if (cb4dto.getSubjectContainerId().split(":").length > 1
						&& cb4dto.getSubjectContainerId().split(":")[1] != null) {
					String xy = cb4dto.getSubjectContainerId().split(":")[1];
					logger.debug(xy.length());
					int xpos=1;
					
					//
					if (xy.length()==4) {
						 xpos = Integer.parseInt(xy.substring(2, 3));
						sourceXPosition = xpos + "".trim();
						sourceYPosition = getValofPositionY(xy.substring(0, 2));
					}
					if (xy.length()==3) {
						 xpos = Integer.parseInt(xy.substring(1, 3));
						sourceXPosition = xpos + "".trim();
						sourceYPosition = getValofPositionY(xy.substring(0, 1));
					}
					//
					logger.debug(Integer.parseInt(sourceXPosition) + "::" + sourceYPosition);
				} else {
					sourceXPosition = position.get("x").toString();
					sourceYPosition = position.get("y").toString();
				}
				System.out.println("sourceXPosition = " + sourceXPosition + " sourceYPosition = " + sourceYPosition);
				
				
				String checkQuery = "SELECT Amount, availableAmount FROM INV_SAMPLE_HOLDER WHERE LABWAREITEMID=? and xposition=? and yposition=?";
				int amount = 0;
				prepStmt = mosaicConnection.prepareStatement(checkQuery);
				prepStmt.setString(1, sourceLabwareItemId);
				prepStmt.setString(2, sourceXPosition);
				prepStmt.setString(3, sourceYPosition);
				rs = prepStmt.executeQuery();
				if (rs.next()) {
					amount = rs.getInt("Amount");
				}
				logger.debug("Solvent amount .." + amount );
				prepStmt.close();
				rs.close();
				
				if (amount==0) {				
					url = url + "/SampleHolders/" + sourceLabwareItemId + "@(" + sourceXPosition + "," + sourceYPosition
						+ ")/AddSolventSample";				
					Double d = 0.0;
					int resolution = 0;
					if (cb4dto.getSolventAmount()!=null  && !cb4dto.getSolventAmount().equalsIgnoreCase("")) {
						d = Double.parseDouble(cb4dto.getSolventAmount());
					}
					BigDecimal b = BigDecimal.valueOf(d);		
					
					if ( b.scale()>=6 ) {
						String str = String.valueOf(d);
						String [] arrStr = str.split("\\.") ;
						String val = arrStr[1].substring(0, 5);
						logger.debug(val);
						String finalStrVal = arrStr[0]+"."+arrStr[1];
						d= Double.parseDouble(finalStrVal);
						logger.debug("final set value to 6 decimails" + d );
						logger.debug("greater than or equal to 6 so setting to 6 only : " + b.scale());
						resolution=-6;
						
					}else {
						logger.debug("less than than 6 so setting to same scale : "  + b.scale());
						resolution = b.scale()*-1;
					}
					
					logger.debug("resolution ... " + resolution); 
					logger.debug("final double value ... " + d); 
					
					StringEntity params = new StringEntity ("{\"solvents\":[{\"solventId\":0,\"proportion\":1}],\"volume\":{\"value\":"+d+",\"displayExponent\":-3,\"resolution\":"+resolution+"}}");
					System.out.println("params : " + params);
					
					jsonResult = getAPIResponse(url, "POST", params);
					logger.debug("SOLVATE ..........jsonResult=" + jsonResult);
			} else {
				System.out.println("inside update solvent since we have added already....");
				url = url + "/SampleHolders/" + sourceLabwareItemId + "@(" + sourceXPosition + "," + sourceYPosition+ ")";				
					Double d = 0.0;
					int resolution = 0;
					if (cb4dto.getSolventAmount()!=null  && !cb4dto.getSolventAmount().equalsIgnoreCase("")) {
						d = Double.parseDouble(cb4dto.getSolventAmount());
					}
					BigDecimal b = BigDecimal.valueOf(d);		
					
					if ( b.scale()>=6 ) {
						String str = String.valueOf(d);
						String [] arrStr = str.split("\\.") ;
						String val = arrStr[1].substring(0, 5);
						logger.debug(val);
						String finalStrVal = arrStr[0]+"."+arrStr[1];
						d= Double.parseDouble(finalStrVal);
						logger.debug("final set value to 6 decimails" + d );
						logger.debug("greater than or equal to 6 so setting to 6 only : " + b.scale());
						resolution=-6;
						
					}else {
						logger.debug("less than than 6 so setting to same scale : "  + b.scale());
						resolution = b.scale()*-1;
					}
					
					logger.debug("resolution ... " + resolution); 
					logger.debug("final double value ... " + d); 
					
					StringEntity params = new StringEntity ("{\"solutionSample\":{\"solvents\":{\"solvents\":[{\"solventId\":0,\"proportion\":1}],\"volume\":{\"value\":"+d+",\"displayExponent\":-3,\"resolution\":"+resolution+"}}}");
					System.out.println("params : " + params);					
					jsonResult = getAPIResponse(url, "POST", params);
					logger.debug("SOLVATE ..........jsonResult=" + jsonResult);
					
			}
			updateQueue(jsonResult, cb4dto);

				return jsonResult;
			} catch (Exception e) {
				if (rs != null && !rs.isClosed())
					rs.close();
				if (prepStmt != null && !prepStmt.isClosed())
					prepStmt.close();
				//throw e;
				logger.debug("------------------------tracking the error.............." + e.getMessage());
				updateQueue(e.getMessage(), cb4dto);
				return e.getMessage();
			} finally {

			}
		}

	
	public static void main(String args[]) throws Exception {
		System.out.println("test--with latest loggers v2.20.0"); 
		ETLJob baseService = new ETLJob();
//		String url = "https://takeda.mosaic-cloud.com/api/Inventory/LabwareItems";
//		String barcode = "sri_test_05052023";
//		int mosaicLabwaretypeId = 4021;
//		String lab = "\\TAKEDA\\TSD\\S.M. Lab";
//		String replaceValue = lab.replace("\\","\\\\");
//		StringEntity params =  new StringEntity("[{\"barcode\": \"" + barcode + "\",\"labwareTypeId\":" + mosaicLabwaretypeId + ",\"location\":{\"locationIdentifier\":{\"locationPath\": \""+replaceValue+ "\"}}}]");
//		
//		
//		System.out.println(params);
//		try {
//			String rslt = baseService.getAPIResponse(url, "POST", params); 
//			System.out.println("rslt : " + rslt);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
		
		baseService.invokeAPIForETL( args[0]);
		

    }
}
