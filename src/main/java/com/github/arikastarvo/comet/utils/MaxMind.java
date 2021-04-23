package com.github.arikastarvo.comet.utils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;

import com.maxmind.db.CHMCache;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;

public class MaxMind {

	private final static String MMASN = "maxmind/GeoLite2-ASN_20200811/GeoLite2-ASN.mmdb";
	private final static String MMCC = "maxmind/GeoLite2-Country_20200814/GeoLite2-Country.mmdb";
	
	private static DatabaseReader asn = null;
	private static DatabaseReader cc = null;
	
	private static void init() {

		if(asn == null) {
			try {
				asn = new DatabaseReader.Builder(MaxMind.class.getClassLoader().getResourceAsStream(MMASN)).withCache(new CHMCache()).build();
			} catch (IOException e) {
				// fail silent?
			}
		}
		if(cc == null) {
			try {
				cc = new DatabaseReader.Builder(MaxMind.class.getClassLoader().getResourceAsStream(MMCC)).withCache(new CHMCache()).build();
			} catch (IOException e) {
				// fail silent?
			}
		}
	}
	
	public static int asn(String ip) {
		init();
		if(asn == null) {
			return 0;
		}
		
		InetAddress ipAddr;
		try {
			ipAddr = InetAddress.getByName(ip);
			return asn(ipAddr);
		} catch (UnknownHostException e) {
			return 0;
		}
	}
	
	public static int asn(InetAddress ip) {
		init();
		if(asn == null) {
			return 0;
		}
		
	    try {
			return asn.asn(ip).getAutonomousSystemNumber();
		} catch (IOException | GeoIp2Exception e) {
			return 0;
		}
	}
	
	public static String asnName(String ip) {
		init();
		if(asn == null) {
			return null;
		}
		
		InetAddress ipAddr;
		try {
			ipAddr = InetAddress.getByName(ip);
			return asnName(ipAddr);
		} catch (UnknownHostException e) {
			return null;
		}
	}
	
	public static String asnName(InetAddress ip) {
		init();
		if(asn == null) {
			return null;
		}
		
	    try {
			return asn.asn(ip).getAutonomousSystemOrganization();
		} catch (IOException | GeoIp2Exception e) {
			return null;
		}
	}
	
	/** CC **/
	
	public static String cc(String ip) {
		init();
		if(cc == null) {
			return null;
		}
		
		InetAddress ipAddr;
		try {
			ipAddr = InetAddress.getByName(ip);
			return cc(ipAddr);
		} catch (UnknownHostException e) {
			return null;
		}
	}
	
	public static String cc(InetAddress ip) {
		init();
		if(cc == null) {
			return null;
		}
		
	    try {
			return cc.country(ip).getCountry().getIsoCode();
		} catch (IOException | GeoIp2Exception e) {
			return null;
		}
	}
	

	public static String ccName(String ip) {
		init();
		if(cc == null) {
			return null;
		}
		
		InetAddress ipAddr;
		try {
			ipAddr = InetAddress.getByName(ip);
			return ccName(ipAddr);
		} catch (UnknownHostException e) {
			return null;
		}
	}
	
	public static String ccName(InetAddress ip) {
		init();
		if(cc == null) {
			return null;
		}
		
	    try {
			return cc.country(ip).getCountry().getName();
		} catch (IOException | GeoIp2Exception e) {
			return null;
		}
	}
}
