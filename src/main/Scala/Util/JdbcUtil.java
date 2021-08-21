/**
 * JdbcUtil.java
 * com.hnxy.utils
 * Copyright (c) 2019, 海牛版权所有.
 * @author   青牛
*/

package Util;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;

import javax.swing.plaf.synth.SynthSpinnerUI;

import com.alibaba.druid.pool.DruidDataSource;

/**
 * 转么用来进行数据库连接的打开与关闭
 * <p>
 * TODO(这里描述这个类补充说明 – 可选)
 * @author   青牛
 * @Date	 2019年5月18日 	 
 */
public class JdbcUtil {

	// JDBC连接数据库的重要四大属性 
	
	// 驱动
	private static final String CONN_DRIVER = "com.mysql.jdbc.Driver";
	
	// URL
	private static final String CONN_URL = "jdbc:mysql:///counter_db?useUnicode=true&amp;characterEncoding=UTF-8";
	
	// 用户名
	private static final String CONN_USER = "root";
	
	// 密码
	private static final String CONN_PASS = "root";
	
	// druid的连接池管理对象
	private static DruidDataSource dataSource = new DruidDataSource();
	
	static{
		// 因为之间没有连接池 所以我们自己通过四大属性进行连接的获取
		// 现在有了连接池 我们可以将四大属性配置给连接池对象 然后我我们通过调用连接池对象来获取我们希望获取的连接
		dataSource.setDriverClassName(CONN_DRIVER);
		dataSource.setUrl(CONN_URL);
		dataSource.setUsername(CONN_USER);
		dataSource.setPassword(CONN_PASS);
	}

	public static DruidDataSource getDataSource() {
		return dataSource;
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	

}
