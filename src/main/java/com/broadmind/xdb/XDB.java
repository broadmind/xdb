//////////////////////////////////////////////////////////////////////////////
//
//  Copyright 2001-2022
//  Broadmind Research Corporation (http://www.broadmind.com)
//
//  Author: Chad Attermann
//  Revision: 
//
//////////////////////////////////////////////////////////////////////////////

package com.broadmind.xdb;

import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;

import com.broadmind.xdb.ConnectionPool;

public class XDB extends java.lang.Object
{

	static final Logger logger = LoggerFactory.getLogger("XDB");

	private String g_strRWDriver = null;
	private String g_strRWURI = null;
	private String g_strRWUsername = null;
	private String g_strRWPassword = null;
	private int g_nRWInitial = 1;
	private int g_nRWMax = 10;
	private boolean g_bRWWait = true;
	private long g_lRWWaitTimeout = 0;
	private String g_strRWInitSql = null;
	private boolean g_bRWCheckValid = false;

	private String g_strRODriver = null;
	private String g_strROURI = null;
	private String g_strROUsername = null;
	private String g_strROPassword = null;
	private int g_nROInitial = 1;
	private int g_nROMax = 10;
	private boolean g_bROWait = true;
	private long g_lROWaitTimeout = 0;
	private String g_strROInitSql = null;
	private boolean g_bROCheckValid = false;

	private String g_strREDriver = null;
	private String g_strREURI = null;
	private String g_strREUsername = null;
	private String g_strREPassword = null;
	private int g_nREInitial = 1;
	private int g_nREMax = 10;
	private boolean g_bREWait = true;
	private long g_lREWaitTimeout = 0;
	private String g_strREInitSql = null;
	private boolean g_bRECheckValid = false;

	private String g_strARDriver = null;
	private String g_strARURI = null;
	private String g_strARUsername = null;
	private String g_strARPassword = null;
	private int g_nARInitial = 1;
	private int g_nARMax = 10;
	private boolean g_bARWait = true;
	private long g_lARWaitTimeout = 0;
	private String g_strARInitSql = null;
	private boolean g_bARCheckValid = false;

	private ConnectionPool m_cRWConnectionPool = null;
	private ConnectionPool m_cROConnectionPool = null;
	private ConnectionPool m_cREConnectionPool = null;
	private ConnectionPool m_cARConnectionPool = null;

	//private String m_strConfFile = "/etc/xdb.conf";
	private String m_strConfFile = "xdb.conf";

	private boolean m_bClearPasswords = true;

	private Properties props = new Properties();

	public XDB() throws Exception
	{
		initialize( m_strConfFile );
	}

	public XDB( String strConfFile ) throws Exception
	{
		m_strConfFile = strConfFile;
		initialize( m_strConfFile );
	}

	public XDB( Properties props ) throws Exception
	{
		initialize( props );
	}

	private void initialize( String strConfFile ) throws Exception
	{

		setConfFile( strConfFile );

		reset();
	}

	private void initialize( Properties props ) throws Exception
	{

		if (props != null)
			this.props = props;

		reset();
	}

	public void reset() throws Exception
	{
		String strValue = null;

		m_cRWConnectionPool = null;
		m_cROConnectionPool = null;
		m_cREConnectionPool = null;
		m_cARConnectionPool = null;

		g_strRWDriver = props.getProperty( "RWDBDriver" );
log( "RWDBDriver=" + g_strRWDriver );
		g_strRWURI = props.getProperty( "RWDBURI" );
log( "RWDBURI=" + g_strRWURI );
		g_strRWUsername = props.getProperty( "RWDBUsername" );
log( "RWDBUsername=" + g_strRWUsername );
		g_strRWPassword = props.getProperty( "RWDBPassword" );
//log( "RWDBPassword=" + g_strRWPassword );
		g_strRWInitSql = props.getProperty( "RWDBInitSql" );
log( "RWDBInitSql=" + g_strRWInitSql );

		g_strRODriver = props.getProperty( "RODBDriver" );
log( "RODBDriver=" + g_strRODriver );
		g_strROURI = props.getProperty( "RODBURI" );
log( "RODBURI=" + g_strROURI );
		g_strROUsername = props.getProperty( "RODBUsername" );
log( "RODBUsername=" + g_strROUsername );
		g_strROPassword = props.getProperty( "RODBPassword" );
//log( "RODBPassword=" + g_strROPassword );
		g_strROInitSql = props.getProperty( "RODBInitSql" );
log( "RODBInitSql=" + g_strROInitSql );

		g_strREDriver = props.getProperty( "REDBDriver" );
log( "REDBDriver=" + g_strREDriver );
		g_strREURI = props.getProperty( "REDBURI" );
log( "REDBURI=" + g_strREURI );
		g_strREUsername = props.getProperty( "REDBUsername" );
log( "REDBUsername=" + g_strREUsername );
		g_strREPassword = props.getProperty( "REDBPassword" );
//log( "REDBPassword=" + g_strREPassword );
		g_strREInitSql = props.getProperty( "REDBInitSql" );
log( "REDBInitSql=" + g_strREInitSql );

		g_strARDriver = props.getProperty( "ARDBDriver" );
log( "ARDBDriver=" + g_strARDriver );
		g_strARURI = props.getProperty( "ARDBURI" );
log( "ARDBURI=" + g_strARURI );
		g_strARUsername = props.getProperty( "ARDBUsername" );
log( "ARDBUsername=" + g_strARUsername );
		g_strARPassword = props.getProperty( "ARDBPassword" );
//log( "ARDBPassword=" + g_strARPassword );
		g_strARInitSql = props.getProperty( "ARDBInitSql" );
log( "ARDBInitSql=" + g_strARInitSql );

		strValue = props.getProperty( "RWDBInitial" );
		if ( strValue != null && strValue.length() > 0 )
			g_nRWInitial = Integer.parseInt( strValue );
		strValue = props.getProperty( "RWDBMax" );
		if ( strValue != null && strValue.length() > 0 )
			g_nRWMax = Integer.parseInt( strValue );
		strValue = props.getProperty( "RWDBWait" );
		if ( strValue != null && strValue.length() > 0 )
			g_bRWWait = Boolean.parseBoolean( strValue );
		strValue = props.getProperty( "RWDBWaitTimeout" );
		if ( strValue != null && strValue.length() > 0 )
			g_lRWWaitTimeout = Long.parseLong( strValue );
		strValue = props.getProperty( "RWDBCheckValid" );
		if ( strValue != null && strValue.length() > 0 )
			g_bRWCheckValid = Boolean.parseBoolean( strValue );

		strValue = props.getProperty( "RODBInitial" );
		if ( strValue != null && strValue.length() > 0 )
			g_nROInitial = Integer.parseInt( strValue );
		strValue = props.getProperty( "RODBMax" );
		if ( strValue != null && strValue.length() > 0 )
			g_nROMax = Integer.parseInt( strValue );
		strValue = props.getProperty( "RODBWait" );
		if ( strValue != null && strValue.length() > 0 )
			g_bROWait = Boolean.parseBoolean( strValue );
		strValue = props.getProperty( "RODBWaitTimeout" );
		if ( strValue != null && strValue.length() > 0 )
			g_lROWaitTimeout = Long.parseLong( strValue );
		strValue = props.getProperty( "ODBCheckValid" );
		if ( strValue != null && strValue.length() > 0 )
			g_bROCheckValid = Boolean.parseBoolean( strValue );

		strValue = props.getProperty( "REDBInitial" );
		if ( strValue != null && strValue.length() > 0 )
			g_nREInitial = Integer.parseInt( strValue );
		strValue = props.getProperty( "REDBMax" );
		if ( strValue != null && strValue.length() > 0 )
			g_nREMax = Integer.parseInt( strValue );
		strValue = props.getProperty( "REDBWait" );
		if ( strValue != null && strValue.length() > 0 )
			g_bREWait = Boolean.parseBoolean( strValue );
		strValue = props.getProperty( "REDBWaitTimeout" );
		if ( strValue != null && strValue.length() > 0 )
			g_lREWaitTimeout = Long.parseLong( strValue );
		strValue = props.getProperty( "REDBCheckValid" );
		if ( strValue != null && strValue.length() > 0 )
			g_bRECheckValid = Boolean.parseBoolean( strValue );

		strValue = props.getProperty( "ARDBInitial" );
		if ( strValue != null && strValue.length() > 0 )
			g_nARInitial = Integer.parseInt( strValue );
		strValue = props.getProperty( "ARDBMax" );
		if ( strValue != null && strValue.length() > 0 )
			g_nARMax = Integer.parseInt( strValue );
		strValue = props.getProperty( "ARDBWait" );
		if ( strValue != null && strValue.length() > 0 )
			g_bARWait = Boolean.parseBoolean( strValue );
		strValue = props.getProperty( "ARDBWaitTimeout" );
		if ( strValue != null && strValue.length() > 0 )
			g_lARWaitTimeout = Long.parseLong( strValue );
		strValue = props.getProperty( "ARDBCheckValid" );
		if ( strValue != null && strValue.length() > 0 )
			g_bARCheckValid = Boolean.parseBoolean( strValue );

		try
		{
			if (g_strRWDriver != null) {
				log("Initializing RW connection pool...");
				m_cRWConnectionPool = new ConnectionPool( g_strRWDriver, g_strRWURI, g_strRWUsername, g_strRWPassword, g_nRWInitial, g_nRWMax, g_bRWWait, g_lRWWaitTimeout, g_strRWInitSql );
				m_cRWConnectionPool.setCheckValid(g_bRWCheckValid);
			}
			if (g_strRODriver != null) {
				log("Initializing RO connection pool...");
				m_cROConnectionPool = new ConnectionPool( g_strRODriver, g_strROURI, g_strROUsername, g_strROPassword, g_nROInitial, g_nROMax, g_bROWait, g_lROWaitTimeout, g_strROInitSql );
				m_cROConnectionPool.setCheckValid(g_bROCheckValid);
			}
			if (g_strREDriver != null) {
				log("Initializing RE connection pool...");
				m_cREConnectionPool = new ConnectionPool( g_strREDriver, g_strREURI, g_strREUsername, g_strREPassword, g_nREInitial, g_nREMax, g_bREWait, g_lREWaitTimeout, g_strREInitSql );
				m_cREConnectionPool.setCheckValid(g_bRECheckValid);
			}
			if (g_strARDriver != null) {
				log("Initializing AR connection pool...");
				m_cARConnectionPool = new ConnectionPool( g_strARDriver, g_strARURI, g_strARUsername, g_strARPassword, g_nARInitial, g_nARMax, g_bARWait, g_lARWaitTimeout, g_strARInitSql );
				m_cARConnectionPool.setCheckValid(g_bARCheckValid);
			}
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			throw e;
		}
	}

	public void setProps(Properties props)
	{
		this.props = props;
	}

	public void setConfFile(String strConfFile) throws Exception
	{
		m_strConfFile = strConfFile;
		props = new Properties();
		try
		{
			FileInputStream fin = new FileInputStream( strConfFile );
			props.load( fin );
			fin.close();
		}
		catch ( Exception e )
		{
			System.out.println( "conf file Exception: " + e.getMessage() );
			throw e;
		}
	}

	public String getConfFile()
	{
		return m_strConfFile;
	}

	public void setClearPasswords( boolean bClearPasswords )
	{
		m_bClearPasswords = bClearPasswords;
	}

	public boolean isClearPasswords()
	{
		return m_bClearPasswords;
	}

	public ConnectionPool getRWConnectionPool()
	{
		return m_cRWConnectionPool;
	}
	public ConnectionPool getROConnectionPool()
	{
		return m_cROConnectionPool;
	}
	public ConnectionPool getREConnectionPool()
	{
		return m_cREConnectionPool;
	}
	public ConnectionPool getARConnectionPool()
	{
		return m_cARConnectionPool;
	}

	public void resetRWConnectionPool() throws Exception
	{
		m_cRWConnectionPool = null;
		reset();
	}
	public void resetROConnectionPool() throws Exception
	{
		m_cROConnectionPool = null;
		reset();
	}
	public void resetREConnectionPool() throws Exception
	{
		m_cREConnectionPool = null;
		reset();
	}
	public void resetARConnectionPool() throws Exception
	{
		m_cARConnectionPool = null;
		reset();
	}

	public String getRWConnectionPoolStatus()
	{
		if ( m_cRWConnectionPool != null )
			return m_cRWConnectionPool.toString();
		else
			return "(null)";
	}
	public String getROConnectionPoolStatus()
	{
		if ( m_cROConnectionPool != null )
			return m_cROConnectionPool.toString();
		else
			return "(null)";
	}
	public String getREConnectionPoolStatus()
	{
		if ( m_cREConnectionPool != null )
			return m_cREConnectionPool.toString();
		else
			return "(null)";
	}
	public String getARConnectionPoolStatus()
	{
		if ( m_cARConnectionPool != null )
			return m_cARConnectionPool.toString();
		else
			return "(null)";
	}


	public java.sql.Connection getRWConnection() throws java.sql.SQLException
	{
		if ( m_cRWConnectionPool == null )
			return null;
		//return m_cRWConnectionPool.getConnection();
		long start = System.nanoTime()/1000000;
		java.sql.Connection con = m_cRWConnectionPool.getConnection();
		m_cRWConnectionPool.incrementSyncWaitTime((long)(System.nanoTime()/1000000 - start));
		return con;
	}
	public void releaseRWConnection( java.sql.Connection con ) throws java.sql.SQLException
	{
		if ( m_cRWConnectionPool == null )
			return;
		if ( con != null )
			m_cRWConnectionPool.free( con );
	}

	public java.sql.Connection getROConnection() throws java.sql.SQLException
	{
		if ( m_cROConnectionPool == null )
			return null;
		//return m_cROConnectionPool.getConnection();
		long start = System.nanoTime()/1000000;
		java.sql.Connection con = m_cROConnectionPool.getConnection();
		m_cROConnectionPool.incrementSyncWaitTime((long)(System.nanoTime()/1000000 - start));
		return con;
	}
	public void releaseROConnection( java.sql.Connection con ) throws java.sql.SQLException
	{
		if ( m_cROConnectionPool == null )
			return;
		if ( con != null )
			m_cROConnectionPool.free( con );
	}

	public java.sql.Connection getREConnection() throws java.sql.SQLException
	{
		if ( m_cREConnectionPool == null )
			return null;
		//return m_cREConnectionPool.getConnection();
		long start = System.nanoTime()/1000000;
		java.sql.Connection con = m_cREConnectionPool.getConnection();
		m_cREConnectionPool.incrementSyncWaitTime((long)(System.nanoTime()/1000000 - start));
		return con;
	}
	public void releaseREConnection( java.sql.Connection con ) throws java.sql.SQLException
	{
		if ( m_cREConnectionPool == null )
			return;
		if ( con != null )
			m_cREConnectionPool.free( con );
	}

	public java.sql.Connection getARConnection() throws java.sql.SQLException
	{
		if ( m_cARConnectionPool == null )
			return null;
		//return m_cARConnectionPool.getConnection();
		long start = System.nanoTime()/1000000;
		java.sql.Connection con = m_cARConnectionPool.getConnection();
		m_cARConnectionPool.incrementSyncWaitTime((long)(System.nanoTime()/1000000 - start));
		return con;
	}
	public void releaseARConnection( java.sql.Connection con ) throws java.sql.SQLException
	{
		if ( m_cARConnectionPool == null )
			return;
		if ( con != null )
			m_cARConnectionPool.free( con );
	}

	public void log( String msg )
	{
		//out.println( new java.sql.Timestamp(System.currentTimeMillis()) + " XDB: " + msg );
	    logger.debug( msg );
	}


	public static long getTime()
	{
		return new GregorianCalendar( TimeZone.getTimeZone( "UTC" ) ).getTime().getTime();
	}

	public static long getDayBeginTime( java.util.Calendar selecteddate )
	{
		return getDayBeginTime( selecteddate, 0 );
	}
	public static long getDayBeginTime( java.util.Calendar selecteddate, int adjustdays )
	{
		java.util.Calendar date = (java.util.Calendar)selecteddate.clone();
		date.set( Calendar.HOUR_OF_DAY, 0 );
		date.set( Calendar.MINUTE, 0 );
		date.set( Calendar.SECOND, 0 );
		date.set( Calendar.MILLISECOND, 0 );
		date.add( java.util.Calendar.DAY_OF_YEAR, adjustdays );
		return date.getTime().getTime();
	}

	public static long getDayEndTime( java.util.Calendar selecteddate )
	{
		return getDayEndTime( selecteddate, 0 );
	}
	public static long getDayEndTime( java.util.Calendar selecteddate, int adjustdays )
	{
		java.util.Calendar date = (java.util.Calendar)selecteddate.clone();
		date.set( Calendar.HOUR_OF_DAY, 23 );
		date.set( Calendar.MINUTE, 59 );
		date.set( Calendar.SECOND, 59 );
		date.set( Calendar.MILLISECOND, 0 );
		date.add( java.util.Calendar.DAY_OF_YEAR, adjustdays );
		return date.getTime().getTime();
	}

	public static long getWeekBeginTime( java.util.Calendar selecteddate )
	{
		return getWeekBeginTime( selecteddate, 0 );
	}
	public static long getWeekBeginTime( java.util.Calendar selecteddate, int adjustweeks )
	{
		java.util.Calendar date = (java.util.Calendar)selecteddate.clone();
		date.set( Calendar.HOUR_OF_DAY, 0 );
		date.set( Calendar.MINUTE, 0 );
		date.set( Calendar.SECOND, 0 );
		date.set( Calendar.MILLISECOND, 0 );
		date.set( Calendar.DAY_OF_WEEK, Calendar.SUNDAY );
		date.add( Calendar.WEEK_OF_YEAR, adjustweeks );
		return date.getTime().getTime();
	}

	public static long getWeekEndTime( java.util.Calendar selecteddate )
	{
		return getWeekEndTime( selecteddate, 0 );
	}
	public static long getWeekEndTime( java.util.Calendar selecteddate, int adjustweeks )
	{
		java.util.Calendar date = (java.util.Calendar)selecteddate.clone();
		date.set( Calendar.HOUR_OF_DAY, 23 );
		date.set( Calendar.MINUTE, 59 );
		date.set( Calendar.SECOND, 59 );
		date.set( Calendar.MILLISECOND, 0 );
		date.set( Calendar.DAY_OF_WEEK, Calendar.SATURDAY );
		date.add( Calendar.WEEK_OF_YEAR, adjustweeks );
		date.add( java.util.Calendar.DAY_OF_YEAR, -1 );
		return date.getTime().getTime();
	}

	public static long getMonthBeginTime( java.util.Calendar selecteddate )
	{
		return getMonthBeginTime( selecteddate, 0 );
	}
	public static long getMonthBeginTime( java.util.Calendar selecteddate, int adjustmonths )
	{
		java.util.Calendar date = (java.util.Calendar)selecteddate.clone();
		date.set( Calendar.HOUR_OF_DAY, 0 );
		date.set( Calendar.MINUTE, 0 );
		date.set( Calendar.SECOND, 0 );
		date.set( Calendar.MILLISECOND, 0 );
		date.set( Calendar.DAY_OF_MONTH, 1 );
		date.add( Calendar.MONTH, adjustmonths );
		return date.getTime().getTime();
	}

	public static long getMonthEndTime( java.util.Calendar selecteddate )
	{
		return getMonthEndTime( selecteddate, 0 );
	}
	public static long getMonthEndTime( java.util.Calendar selecteddate, int adjustmonths )
	{
		java.util.Calendar date = (java.util.Calendar)selecteddate.clone();
		date.set( Calendar.HOUR_OF_DAY, 23 );
		date.set( Calendar.MINUTE, 59 );
		date.set( Calendar.SECOND, 59 );
		date.set( Calendar.MILLISECOND, 0 );
		// CBA First of next month minus one day equals end of this month
		date.set( Calendar.DAY_OF_MONTH, 1 );
		date.add( Calendar.MONTH, adjustmonths + 1 );
		date.add( java.util.Calendar.DAY_OF_YEAR, -1 );
		return date.getTime().getTime();
	}

}
