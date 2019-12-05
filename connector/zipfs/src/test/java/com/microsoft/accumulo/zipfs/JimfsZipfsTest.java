/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.accumulo.zipfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.google.common.io.Resources;
import java.nio.file.*;
import org.junit.Test;
import java.util.*;

public class JimfsZipfsTest {
	@Test
	public void testJimfsAndZipFs() throws Exception {
		// read .zip file into memory
		byte[] data = Resources.toByteArray(JimfsZipfsTest.class.getResource("sample.zip"));

		// create in-memory filesystem
		FileSystem fs = Jimfs.newFileSystem(Configuration.unix());
		Path sampleFilePath = fs.getPath("/sample.zip");

		Files.write(sampleFilePath, data, StandardOpenOption.CREATE);

		// get zip file system
		ZipFileSystem zfs = new ZipFileSystem(new ZipFileSystemProvider(), sampleFilePath, new HashMap<String, Object>());

		Path pathInZip = zfs.getPath("/sample.txt");

		List<String> lines = Files.readAllLines(pathInZip);

		assertEquals(1, lines.size());
		assertEquals("Hello World", lines.get(0));
	}
}