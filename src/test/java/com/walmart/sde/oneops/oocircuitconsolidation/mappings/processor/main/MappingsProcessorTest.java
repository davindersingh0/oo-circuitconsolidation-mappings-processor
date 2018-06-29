package com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.main;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.google.gson.Gson;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.config.IConstants;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.config.PostgressDbConnection;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.util.CircuitconsolidationUtil;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.util.MappingsCache;


public class MappingsProcessorTest {


  String host;
  String user;
  String pwd;

  private final Logger log = LoggerFactory.getLogger(getClass());


  private Gson gson = new Gson();

  @BeforeClass
  private void init() {

    host = System.getenv().get(IConstants.CMS_DB_HOST);
    user = System.getenv().get(IConstants.CMS_DB_USER);
    pwd = System.getenv().get(IConstants.CMS_DB_PASS);



  }


  @Test(enabled = true)
  private void testMappingsConfigFile() throws SQLException {
    
    String ns="/TestOrg2/guineapigs1";
    String platformName="guineapig-brown";
    String ooPhase=IConstants.DESIGN_PHASE;
    String envName=null; // null for design phase
    
    MappingsProcessorMain mappingsProcessorMain= new MappingsProcessorMain();
    
    
    Properties props = mappingsProcessorMain.loadTransformationConfigurations(host, user, pwd);
    Connection conn = PostgressDbConnection.getConnection(props);

    MappingsCache mappingsCache= new MappingsCache();
    
    Map<String, List> transformationMappings = mappingsCache.createTransformationMappingsCache(conn);
    log.info("transformationMappings: "+gson.toJson(transformationMappings));

    
    mappingsProcessorMain.processMappings(transformationMappings, ns, platformName, ooPhase, envName, conn);

  }




}
