package com.github.arikastarvo.comet.utils;

import java.net.URI;

public class FileSystem {

	
	public static String getPathFromURI(URI inputDefinition) {
		
		String path;
		if(inputDefinition.isOpaque() && inputDefinition.getSchemeSpecificPart().length() > 0 ) {
			// opaque URI (in the sense of URI SPEC)
			path = inputDefinition.getSchemeSpecificPart();
		} else if (inputDefinition.getPath().length() > 0) {
			// just absolute uri URI (not opaque?)
			path = inputDefinition.getPath();
		} else {
			// this is fallback just put the original for  now. maybe we should throw error?
			path = inputDefinition.getSchemeSpecificPart();
		}
		return path;
	}
}
