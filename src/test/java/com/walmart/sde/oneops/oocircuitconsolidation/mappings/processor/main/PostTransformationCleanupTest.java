package com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.main;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.google.gson.Gson;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.config.IConstants;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.config.PostgressDbConnection;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.dal.KloopzCmDal;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.util.CircuitconsolidationUtil;

public class PostTransformationCleanupTest {

  private final Logger log = LoggerFactory.getLogger(getClass());


  private String host;
  private String user;
  private String pwd;
  private PostTransformationCleanup postTransformationCleanup = new PostTransformationCleanup();

  private String ns = "/TestOrg2/TestTransformtionOperatePhase27";
  private String platformName = "guineapig-brown";
  private String envName = "dev"; // null for design phase
  String ooPhase = IConstants.DESIGN_PHASE;

  String nsForPlatformCiComponents;
  private Properties props;
  private Connection conn;
  private KloopzCmDal dal;

  private Gson gson;

  @BeforeClass
  private void init() {
    gson = new Gson();
    props = new Properties();
    host = System.getenv().get(IConstants.CMS_DB_HOST);
    user = System.getenv().get(IConstants.CMS_DB_USER);
    pwd = System.getenv().get(IConstants.CMS_DB_PASS);

    props.setProperty(IConstants.CMS_DB_HOST, host);
    props.setProperty(IConstants.CMS_DB_USER, user);
    props.setProperty(IConstants.CMS_DB_PASS, pwd);

    conn = PostgressDbConnection.getConnection(props);

    dal = new KloopzCmDal(conn);
    nsForPlatformCiComponents =
        CircuitconsolidationUtil.getnsForPlatformCiComponents(ns, platformName, ooPhase, envName);
    postTransformationCleanup.setPostTransformationCleanupProperties(ooPhase, dal, ns, platformName, envName,
        nsForPlatformCiComponents);

  }


  @Test(enabled = true)
  private void cleanUpPlatformToCIRequiresRelationTest() {

    postTransformationCleanup.cleanUpPlatformToCIRequiresRelation();


  }

  @Test(enabled = false)
  private void cleanUpDependsOnRelationTest() {

    postTransformationCleanup.cleanUpDependsOnRelation();

  }

  @AfterSuite
  private void cleanup() {
    try {
      if (!conn.isClosed()) {
        log.info("Closing DB connection after executing test cases..");
        //conn.commit();
        conn.close();
        log.info("DB connection closed");
      } else {
        log.info("DB connection already closed"); 
        
      }

    } catch (SQLException e) {

      log.info("exception while closing connection");
      e.printStackTrace();
    }

  }

}
