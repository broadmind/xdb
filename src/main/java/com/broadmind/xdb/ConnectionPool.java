//////////////////////////////////////////////////////////////////////////////
//
//	Copyright 2001-2022
//	Broadmind Research Corporation (http://www.broadmind.com)
//
//	Author: Chad Attermann
//	Revision: 
//
//////////////////////////////////////////////////////////////////////////////

package com.broadmind.xdb;

import java.sql.*;
import java.util.*;
import javax.xml.bind.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@XmlAccessorType(XmlAccessType.NONE)
@XmlRootElement(name = "pool")
//public class ConnectionPool implements Runnable, java.io.Serializable {
public class ConnectionPool implements Runnable {

  static final Logger logger = LoggerFactory.getLogger("ConnectionPool");

  private String driver, url, username, password;
  private int maxConnections;
  private boolean waitIfBusy;
  private long waitTimeout = 0;
  private Vector<Connection> availableConnections, busyConnections;
  private boolean connectionPending = false;
  private String initSql = null;
  private boolean checkValid = false;

  // stats
  private long waitCount = 0;
  private long waitTime = 0;
  private long syncWaitTime = 0;

  @SuppressWarnings("unused")
  private ConnectionPool() {
  }

  public ConnectionPool(String driver, String url,
                        String username, String password,
                        int initialConnections,
                        int maxConnections,
                        boolean waitIfBusy,
                        long waitTimeout,
                        String initSql)
      throws SQLException {
    this.driver = driver;
    this.url = url;
    this.username = username;
    this.password = password;
    this.maxConnections = maxConnections;
    this.waitIfBusy = waitIfBusy;
    this.waitTimeout = waitTimeout;
    this.initSql = initSql;
    if (initialConnections > maxConnections) {
      initialConnections = maxConnections;
    }
    availableConnections = new Vector<Connection>(initialConnections);
    busyConnections = new Vector<Connection>();
    for(int i=0; i<initialConnections; i++) {
      availableConnections.addElement(makeNewConnection());
    }
  }

  public ConnectionPool(String driver, String url,
                        String username, String password,
                        int initialConnections,
                        int maxConnections,
                        boolean waitIfBusy,
                        long waitTimeout)
      throws SQLException {
    this(driver, url, username, password, initialConnections, maxConnections, waitIfBusy, waitTimeout, null);
  }

  public synchronized long getWaitCount() { return waitCount; }
  public synchronized long getWaitTime() { return waitTime; }

  public synchronized void incrementSyncWaitTime(long increment) { syncWaitTime += increment; }
  public synchronized long getSyncWaitTime() { return syncWaitTime; }

  @XmlElement(name = "url")
  public synchronized String getUrl() {
    return url;
  }

  @XmlElement(name = "username")
  public synchronized String getUsername() {
    return username;
  }

  @XmlElement(name = "available")
  public synchronized int getAvailableConnections() {
    return availableConnections.size();
  }

  @XmlElement(name = "busy")
  public synchronized int getBusyConnections() {
    return busyConnections.size();
  }

  @XmlElement(name = "total")
  public synchronized int getTotalConnections() {
    return (availableConnections.size() + busyConnections.size());
  }

  @XmlElement(name = "max")
  public synchronized int getMaxConnections() {
    return maxConnections;
  }

  @XmlElement(name = "waittimeout")
  public synchronized long getWaitTimeout() {
    return waitTimeout;
  }

  @XmlElement(name = "init")
  public synchronized String getInitSql() {
    return initSql;
  }

  public synchronized void setCheckValid(boolean checkValid) {
    this.checkValid = checkValid;
  }
  public synchronized boolean getCheckValid() {
    return this.checkValid;
  }

  private boolean isConnectionValid(Connection connection) {
      // CBA Note that isClosed() does not necessarily test connection validity,
      //  so isValid() is also required.
      try {
        if (connection.isClosed()) {
          log("Connection is closed");
          return false;
        }
        if (checkValid) {
          if (!connection.isValid(1)) {
            log("Connection is invalid");
            return false;
          }
        }
        return true;
      }
      catch (SQLException ex) {
        log("Connection validity check exception");
        return false;
      }
  }

  public Connection getConnection()
      throws SQLException {
    return getConnection(System.nanoTime()/1000000, 0);
  }

/*
  private synchronized Connection getConnection(long start, int iteration)
      throws SQLException {
    if (!availableConnections.isEmpty()) {
      Connection existingConnection = (Connection)availableConnections.lastElement();
      int lastIndex = availableConnections.size() - 1;
      availableConnections.removeElementAt(lastIndex);

      // If connection on available list is closed (e.g.,
      // it timed out), then remove it from available list
      // and repeat the process of obtaining a connection.
      // Also wake up threads that were waiting for a
      // connection because maxConnection limit was reached.
      if (!isConnectionValid(existingConnection)) {
        ++waitCount;
        notifyAll(); // Freed up a spot for anybody waiting
        return getConnection(start, iteration+1);
      }
      else {
        busyConnections.addElement(existingConnection);
        waitTime += (long)(System.nanoTime()/1000000 - start);
        return existingConnection;
      }
    }
    else {
      ++waitCount;
      
      // Three possible cases:
      // 1) You haven't reached maxConnections limit. So
      //    establish one in the background if there isn't
      //    already one pending, then wait for
      //    the next available connection (whether or not
      //    it was the newly established one).
      // 2) You reached maxConnections limit and waitIfBusy
      //    flag is false. Throw SQLException in such a case.
      // 3) You reached maxConnections limit and waitIfBusy
      //    flag is true. Then do the same thing as in second
      //    part of step 1: wait for next available connection.
      
      if ((getTotalConnections() < maxConnections) && !connectionPending) {
        makeBackgroundConnection();
      }
      else if (!waitIfBusy) {
        log("getConnection: Connection limit reached, throwing exception");
        waitTime += (long)(System.nanoTime()/1000000 - start);
        throw new SQLException("Connection limit reached");
      }
      // Wait for either a new connection to be established
      // (if you called makeBackgroundConnection) or for
      // an existing connection to be freed up.
      try {
        //ThreadLocal<Long> threadLocalValue = new ThreadLocal<>();
//log("getConnection: waiting...");
        wait(waitTimeout);
//log("getConnection: finished waiting");
//log("getConnection: waitTime=" + (long)(System.nanoTime()/1000000 - start));
//log("getConnection: iteration=" + iteration);
        if (waitTimeout != 0 && (long)(System.nanoTime()/1000000 - start) >= waitTimeout) {
          log("getConnection: Connection wait timeout, throwing exception");
       	  waitTime += (long)(System.nanoTime()/1000000 - start);
          throw new SQLException("Connection wait timeout");
        }
      }
      catch(InterruptedException ie) {
        log("getConnection: EXCEPTION: " + ie.getMessage());
      }
      // Someone freed up a connection, so try again.
      return getConnection(start, iteration+1);
    }
  }
*/

/**/
  private Connection getConnection(long start, int iteration)
      throws SQLException {

	Connection con = null;

	synchronized(this) {

      while (!availableConnections.isEmpty()) {
        con = (Connection)availableConnections.lastElement();
        int lastIndex = availableConnections.size() - 1;
        availableConnections.removeElementAt(lastIndex);

        // If connection on available list is closed (e.g.,
        // it timed out), then remove it from available list
        // and repeat the process of obtaining a connection.
        // Also wake up threads that were waiting for a
        // connection because maxConnection limit was reached.
        if (!isConnectionValid(con)) {
          // close bad connection just in case
          con.close();
          con = null;
        }
        else {
          busyConnections.addElement(con);
          waitTime += (long)(System.nanoTime()/1000000 - start);
          return con;
        }
      }

      // No available connection
      ++waitCount;

      // Three possible cases:
      // 1) You haven't reached maxConnections limit. So
      //    establish one in the background if there isn't
      //    already one pending, then wait for
      //    the next available connection (whether or not
      //    it was the newly established one).
      // 2) You reached maxConnections limit and waitIfBusy
      //    flag is false. Throw SQLException in such a case.
      // 3) You reached maxConnections limit and waitIfBusy
      //    flag is true. Then do the same thing as in second
      //    part of step 1: wait for next available connection.

      if ((getTotalConnections() < maxConnections) && !connectionPending) {
        makeBackgroundConnection();
      }
      else if (!waitIfBusy) {
        log("getConnection: Connection limit reached, throwing exception");
        waitTime += (long)(System.nanoTime()/1000000 - start);
        throw new SQLException("Connection limit reached");
      }

      // Wait for either a new connection to be established
      // (if you called makeBackgroundConnection) or for
      // an existing connection to be freed up.
      try {
//log("getConnection: waiting...");
        wait(waitTimeout);
//log("getConnection: finished waiting");
//log("getConnection: waitTime=" + (long)(System.nanoTime()/1000000 - start));
//log("getConnection: iteration=" + iteration);
        if (waitTimeout != 0 && (long)(System.nanoTime()/1000000 - start) >= waitTimeout) {
          log("getConnection: Connection wait timeout, throwing exception");
          waitTime += (long)(System.nanoTime()/1000000 - start);
          throw new SQLException("Connection wait timeout");
        }
      }
      catch(InterruptedException ie) {
        log("getConnection: EXCEPTION: " + ie.getMessage());
      }

	}

    // Someone freed up a connection, so try again.
    return getConnection(start, iteration+1);
  }
/**/

  // You can't just make a new connection in the foreground
  // when none are available, since this can take several
  // seconds with a slow network connection. Instead,
  // start a thread that establishes a new connection,
  // then wait. You get woken up either when the new connection
  // is established or if someone finishes with an existing
  // connection.

  private void makeBackgroundConnection() {
    connectionPending = true;
    try {
      Thread connectThread = new Thread(this);
      connectThread.start();
    }
    catch(OutOfMemoryError oome) {
      // Give up on new connection
      log("makeBackgroundConnection: EXCEPTION: " + oome.getMessage());
    }
  }

  public void run() {
    try {
      Connection connection = makeNewConnection();
log("run: connection=" + connection);
      if (connection != null) {
        synchronized(this) {
          availableConnections.addElement(connection);
        }
      }
    }
    catch(Exception e) { // SQLException or OutOfMemory
      // Give up on new connection and wait for existing one
      // to free up.
      log("run: EXCEPTION: " + e.getMessage());
    }

    // CBA Need to clear connectionPending flag to avoid hang, and notify waiting threads so that they can attempt a new connection if necessary
    synchronized(this) {
      connectionPending = false;
      notifyAll();
    }
  }

  // This explicitly makes a new connection. Recorded in
  // the foreground when initializing the ConnectionPool,
  // and called in the background when running.
  
  private Connection makeNewConnection()
      throws SQLException {
    try {
      // Load database driver if not already loaded
      //Class.forName(driver);
      Class.forName(driver).newInstance();
      // Establish network connection to database
      Connection connection =
        DriverManager.getConnection(url, username, password);
      if (initSql != null) {
        try (Statement stmt = connection.createStatement()) {
          stmt.execute(initSql);
        }
      }
      return connection;
    }
    catch(ClassNotFoundException cnfe) {
      // Simplify try/catch blocks of people using this by
      // throwing only one exception type.
      log("makeNewConnection: EXCEPTION: " + cnfe.getMessage());
      throw new SQLException("ClassNotFoundException for driver: " + driver);
    }
    catch(InstantiationException ie) {
      // Simplify try/catch blocks of people using this by
      // throwing only one exception type.
      log("makeNewConnection: EXCEPTION: " + ie.getMessage());
      throw new SQLException("InstantiationException for driver: " + driver);
    }
    catch(IllegalAccessException iae) {
      // Simplify try/catch blocks of people using this by
      // throwing only one exception type.
      log("makeNewConnection: EXCEPTION: " + iae.getMessage());
      throw new SQLException("IllegalAccessException for driver: " + driver);
    }
  }

  public synchronized void free(Connection connection) {
    busyConnections.removeElement(connection);
    availableConnections.addElement(connection);
    // Wake up threads that are waiting for a connection
    notifyAll(); 
  }

  /** Close all the connections. Use with caution:
   *  be sure no connections are in use before
   *  calling. Note that you are not <I>required</I> to
   *  call this when done with a ConnectionPool, since
   *  connections are guaranteed to be closed when
   *  garbage collected. But this method gives more control
   *  regarding when the connections are closed.
   */

  public synchronized void closeAllConnections() {
    closeConnections(availableConnections);
    availableConnections = new Vector<Connection>();
    closeConnections(busyConnections);
    busyConnections = new Vector<Connection>();
  }

  private void closeConnections(Vector<Connection> connections) {
    try {
      for(int i=0; i<connections.size(); i++) {
        Connection connection = (Connection)connections.elementAt(i);
        if (!connection.isClosed()) {
          connection.close();
        }
      }
    }
    catch(SQLException sqle) {
      // Ignore errors; garbage collect anyhow
      log("closeConnections: EXCEPTION: " + sqle.getMessage());
    }
  }

  public synchronized String toString() {
    String info =
      "ConnectionPool(" + url + "," + username + ")" +
      ", available=" + availableConnections.size() +
      ", busy=" + busyConnections.size() +
      ", max=" + maxConnections +
      ", init=" + initSql;
    return info;
  }

  public void log( String msg ) {
    //out.println( new java.sql.Timestamp(System.currentTimeMillis()) + " [" + Thread.currentThread().getName() + "] ConnectionPool: " + msg );
    logger.debug( msg );
  }

}
