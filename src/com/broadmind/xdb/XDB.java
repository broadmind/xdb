package com.broadmind.xdb;

import java.util.*;
import java.io.*;

import com.broadmind.xdb.ConnectionPool;

public class XDB extends java.lang.Object
{

	private String g_strRWDriver = null;
	private String g_strRWURI = null;
	private String g_strRWUsername = null;
	private String g_strRWPassword = null;
	private int g_nRWInitial = 1;
	private int g_nRWMax = 10;

	private String g_strRODriver = null;
	private String g_strROURI = null;
	private String g_strROUsername = null;
	private String g_strROPassword = null;
	private int g_nROInitial = 1;
	private int g_nROMax = 10;

	private String g_strREDriver = null;
	private String g_strREURI = null;
	private String g_strREUsername = null;
	private String g_strREPassword = null;
	private int g_nREInitial = 1;
	private int g_nREMax = 10;

	private String g_strARDriver = null;
	private String g_strARURI = null;
	private String g_strARUsername = null;
	private String g_strARPassword = null;
	private int g_nARInitial = 1;
	private int g_nARMax = 10;

	private ConnectionPool m_cRWConnectionPool = null;
	private ConnectionPool m_cROConnectionPool = null;
	private ConnectionPool m_cREConnectionPool = null;
	private ConnectionPool m_cARConnectionPool = null;

	//private String m_strConfFile = "/etc/xdb.conf";
	private String m_strConfFile = "xdb.conf";

	private String m_strLogFile = null;

	private boolean m_bClearPasswords = true;

	public PrintWriter out;

	public XDB() throws Exception
	{
		initialize( m_strConfFile );
	}

	public XDB( String strConfFile ) throws Exception
	{
		m_strConfFile = strConfFile;
		initialize( m_strConfFile );
	}

	private void initialize( String strConfFile ) throws Exception
	{

		Properties props = new Properties();
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

		String strValue = null;

		strValue = props.getProperty( "LogFile" );
		if ( strValue != null )
			m_strLogFile = strValue;
		setLogFile( m_strLogFile );

		g_strRWDriver = props.getProperty( "RWDBDriver" );
log( "RWDBDriver=" + g_strRWDriver );
		g_strRWURI = props.getProperty( "RWDBURI" );
log( "RWDBURI=" + g_strRWURI );
		g_strRWUsername = props.getProperty( "RWDBUsername" );
log( "RWDBUsername=" + g_strRWUsername );
		g_strRWPassword = props.getProperty( "RWDBPassword" );
log( "RWDBPassword=" + g_strRWPassword );

		g_strRODriver = props.getProperty( "RODBDriver" );
log( "RODBDriver=" + g_strRODriver );
		g_strROURI = props.getProperty( "RODBURI" );
log( "RODBURI=" + g_strROURI );
		g_strROUsername = props.getProperty( "RODBUsername" );
log( "RODBUsername=" + g_strROUsername );
		g_strROPassword = props.getProperty( "RODBPassword" );
log( "RODBPassword=" + g_strROPassword );

		g_strREDriver = props.getProperty( "REDBDriver" );
log( "REDBDriver=" + g_strREDriver );
		g_strREURI = props.getProperty( "REDBURI" );
log( "REDBURI=" + g_strREURI );
		g_strREUsername = props.getProperty( "REDBUsername" );
log( "REDBUsername=" + g_strREUsername );
		g_strREPassword = props.getProperty( "REDBPassword" );

		g_strARDriver = props.getProperty( "ARDBDriver" );
log( "ARDBDriver=" + g_strARDriver );
		g_strARURI = props.getProperty( "ARDBURI" );
log( "ARDBURI=" + g_strARURI );
		g_strARUsername = props.getProperty( "ARDBUsername" );
log( "ARDBUsername=" + g_strARUsername );
		g_strARPassword = props.getProperty( "ARDBPassword" );

		strValue = props.getProperty( "RWDBInitial" );
		if ( strValue != null && strValue.length() > 0 )
			g_nRWInitial = Integer.parseInt( strValue );
		strValue = props.getProperty( "RWDBMax" );
		if ( strValue != null && strValue.length() > 0 )
			g_nRWMax = Integer.parseInt( strValue );

		strValue = props.getProperty( "RODBInitial" );
		if ( strValue != null && strValue.length() > 0 )
			g_nROInitial = Integer.parseInt( strValue );
		strValue = props.getProperty( "RODBMax" );
		if ( strValue != null && strValue.length() > 0 )
			g_nROMax = Integer.parseInt( strValue );

		strValue = props.getProperty( "REDBInitial" );
		if ( strValue != null && strValue.length() > 0 )
			g_nREInitial = Integer.parseInt( strValue );
		strValue = props.getProperty( "REDBMax" );
		if ( strValue != null && strValue.length() > 0 )
			g_nREMax = Integer.parseInt( strValue );

		strValue = props.getProperty( "ARDBInitial" );
		if ( strValue != null && strValue.length() > 0 )
			g_nARInitial = Integer.parseInt( strValue );
		strValue = props.getProperty( "ARDBMax" );
		if ( strValue != null && strValue.length() > 0 )
			g_nARMax = Integer.parseInt( strValue );

		try
		{
			m_cRWConnectionPool = new ConnectionPool( g_strRWDriver, g_strRWURI, g_strRWUsername, g_strRWPassword, g_nRWInitial, g_nRWMax, false );
			m_cROConnectionPool = new ConnectionPool( g_strRODriver, g_strROURI, g_strROUsername, g_strROPassword, g_nROInitial, g_nROMax, false );
			m_cREConnectionPool = new ConnectionPool( g_strREDriver, g_strREURI, g_strREUsername, g_strREPassword, g_nREInitial, g_nREMax, false );
			m_cARConnectionPool = new ConnectionPool( g_strARDriver, g_strARURI, g_strARUsername, g_strARPassword, g_nARInitial, g_nARMax, false );
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			throw e;
		}
	}

	public String getConfFile()
	{
		return m_strConfFile;
	}

	public void setLogFile( String strLogFile ) throws Exception
	{
		m_strLogFile = strLogFile;
		if ( m_strLogFile != null && m_strLogFile.length() > 0 )
		{
			try
			{
				out = new PrintWriter( new FileOutputStream( m_strLogFile, true ), true );
			}
			catch ( Exception e )
			{
				out = new PrintWriter( System.out, true );
			}
		}
		else
		{
			out = new PrintWriter( System.out, true );
		}
	}

	public String getLogFile()
	{
		return m_strLogFile;
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
		initialize( m_strConfFile );
	}
	public void resetROConnectionPool() throws Exception
	{
		m_cROConnectionPool = null;
		initialize( m_strConfFile );
	}
	public void resetREConnectionPool() throws Exception
	{
		m_cREConnectionPool = null;
		initialize( m_strConfFile );
	}
	public void resetARConnectionPool() throws Exception
	{
		m_cARConnectionPool = null;
		initialize( m_strConfFile );
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

	public void reset() throws Exception
	{
		m_cRWConnectionPool = null;
		m_cROConnectionPool = null;
		m_cREConnectionPool = null;
		m_cARConnectionPool = null;

		initialize( m_strConfFile );
	}


	public java.sql.Connection getRWConnection() throws java.sql.SQLException
	{
		if ( m_cRWConnectionPool == null )
			return null;
		return m_cRWConnectionPool.getConnection();
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
		return m_cROConnectionPool.getConnection();
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
		return m_cREConnectionPool.getConnection();
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
		return m_cARConnectionPool.getConnection();
	}
	public void releaseARConnection( java.sql.Connection con ) throws java.sql.SQLException
	{
		if ( m_cARConnectionPool == null )
			return;
		if ( con != null )
			m_cARConnectionPool.free( con );
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

	public void log( String msg )
	{
		out.println( new java.sql.Timestamp(System.currentTimeMillis()) + " " + msg );
	}

}
