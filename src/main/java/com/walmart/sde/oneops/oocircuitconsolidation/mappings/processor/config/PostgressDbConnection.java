package com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgressDbConnection {

  private final static Logger log = LoggerFactory.getLogger(PostgressDbConnection.class);

  static Connection connection;

  public static Connection getConnection(Properties props) {
    if (connection == null) {
      createConnection(props);
    }
    return connection;
  }


  private static void createConnection(Properties props) {

    try {
      log.info("creating database connection...");

      String CMS_DB_HOST = props.getProperty(IConstants.CMS_DB_HOST);
      String CMS_DB_USER = props.getProperty(IConstants.CMS_DB_USER);
      String CMS_DB_PASS = props.getProperty(IConstants.CMS_DB_PASS);

      log.info("connecting to <CMS_DB_HOST>:{} for user <CMS_DB_USER>: {}" , CMS_DB_HOST,CMS_DB_USER);
    
        
        
      //Connection conn = DriverManager.getConnection("+, cmsDbUserName, cmsDbPassword)
          
      Class.forName("org.postgresql.Driver");
      connection = DriverManager.getConnection("jdbc:postgresql://"+CMS_DB_HOST+":5432/"+IConstants.CMS_DB_SCHEMA, CMS_DB_USER,
          CMS_DB_PASS);

      log.info("created database connection...");


    } catch (ClassNotFoundException | SQLException e) {

      throw new RuntimeException("Unable to connect to database: " + e.getMessage());
    }


  }



}
