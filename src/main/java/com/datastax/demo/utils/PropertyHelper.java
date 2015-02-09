package com.datastax.demo.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyHelper {
	
	private static Properties prop = new Properties();
	
	public static String getProperty(String name, String defaultValue){		
		return System.getProperty(name) == null ? 
				(prop.getProperty(name) == null ?  defaultValue : prop.getProperty(name)) 
					: System.getProperty(name); 
	}	
	
	static {
		
		String propFileName = "app.properties";
 
		InputStream inputStream = PropertyHelper.class.getClassLoader().getResourceAsStream(propFileName);
 
		try {
			prop.load(inputStream);
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException (e);
		}
	 
	}
}
