package com.github.arikastarvo.comet.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

import java.net.URI;

public class FileSystemUtilsTest {

    @Test
    public void ParseURIPathTest() throws Exception {
		assertEquals("filename.log", FileSystem.getPathFromURI(new URI("filename.log")));
		assertEquals("filename.log", FileSystem.getPathFromURI(new URI("file:filename.log")));
		assertEquals("/filename.log", FileSystem.getPathFromURI(new URI("file:/filename.log")));
		assertEquals("/filename.log", FileSystem.getPathFromURI(new URI("file:///filename.log")));
	}
}
