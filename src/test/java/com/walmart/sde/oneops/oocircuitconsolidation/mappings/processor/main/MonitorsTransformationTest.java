package com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.main;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.google.gson.Gson;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.config.IConstants;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.config.PostgressDbConnection;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.dal.KloopzCmDal;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.util.CircuitconsolidationUtil;

public class MonitorsTransformationTest {


  private final Logger log = LoggerFactory.getLogger(getClass());


  private String host;
  private String user;
  private String pwd;
  private MonitorsTransformation monitorsTransformation = new MonitorsTransformation();

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
    monitorsTransformation.setMonitorsTransformationProperties(ooPhase, dal, ns, platformName,
        envName, nsForPlatformCiComponents);

  }

  @Test(enabled = false)
  private void cleanUpPlatformToCIRequiresRelationTest() {

    try {


      log.info("Starting monitors transformation....");
      monitorsTransformation.transformMonitors();
      log.info("moinotrs trasformation complete for ooPhase {}", ooPhase);

      //conn.commit();
      if (!conn.isClosed()) {

        conn.close();

      }

    } catch (SQLException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();

    }

  }

  @Test(enabled = true)
  private void deleteAllMonitorsTest() {

    try {

      log.info("Starting monitors transformation....");
      monitorsTransformation.deleteAllMonitors();
      log.info("moinotrs trasformation complete for ooPhase {}", ooPhase);

      //conn.commit();
      if (!conn.isClosed()) {

        conn.close();

      }

    } catch (SQLException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();

    }

  }
  
}
