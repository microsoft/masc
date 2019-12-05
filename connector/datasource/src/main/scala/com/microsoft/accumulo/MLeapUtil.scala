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

package com.microsoft.accumulo

import org.apache.spark.sql.types.StructField
import ml.combust.bundle.BundleFile
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.mleap.runtime.MleapContext.defaultContext
import org.apache.spark.sql.mleap.TypeConverters
import java.io.File
import java.util.{Base64, HashMap}
import java.net.URI
import java.nio.file.{Files, FileSystem, FileSystems, Path, StandardOpenOption}
import resource._
import ml.combust.mleap.core.types.ScalarType
import com.google.common.jimfs.{Jimfs, Configuration}
import com.microsoft.accumulo.zipfs.{ZipFileSystem, ZipFileSystemProvider}

@SerialVersionUID(1L)
object MLeapUtil {

	// load the Spark pipeline we saved in the previous section
	def mleapSchemaToCatalyst(modelBase64: String): Seq[StructField] = {
		if (modelBase64.isEmpty)
			Seq.empty[StructField]
		else {
			val mleapBundleArr = Base64.getDecoder().decode(modelBase64)

			val fs = Jimfs.newFileSystem(Configuration.unix())
			val mleapFilePath = fs.getPath("/mleap.zip")
			Files.write(mleapFilePath, mleapBundleArr, StandardOpenOption.CREATE)

			// Why do we access a private constructor???
			// 1. MLeap only exposes a FileSystem layer to load models.
			// 2. We don't want to write to the local file system
			// 2a. We use Google JimFS
			// 2b. We can't use https://github.com/marschall/memoryfilesystem at it has a 16MB file size limitation
			// 2c. We can't use Apache common-vfs as it doesn't support directory listing
			// 3. Usually one triggers the ZFS implementation by prefixing the URI with jar:
			//    Unfortunately on Spark the file system provider disappears from the installed list https://stackoverflow.com/questions/39500445/filesystem-provider-disappearing-in-spark
			//    thus it cannot be found by the ZFS implementation when looking up the jimfs: protocol
			// 4. The public methods (e.g. FileSystems.newFileSystem(), new ZipFileSystemProvider().newFileSystem()) have checks that limit the incoming FileSystemProvider

			// Attempt 10: try to find the jar provider, but then we don't know if the same methods exists :(
			// val zfsProvider = FileSystemProvider.installedProviders().asScala.filter(_.getScheme == "jar")
			// FileSystemProvider.installedProviders().asScala.foreach(p => println(p.getScheme))

			// Attempt 9: hard dependency on Oracle JDK, fails on OpenJDK
			// package private ctor... *sigh*
			// import com.sun.nio.zipfs.{ZipFileSystem, ZipFileSystemProvider}
			// val zfsCtor = classOf[ZipFileSystem].getDeclaredConstructor(
				// classOf[ZipFileSystemProvider], 
				// classOf[java.nio.file.Path], 
				// classOf[java.util.Map[String, Object]])

			// zfsCtor.setAccessible(true)
			// val zfs = zfsCtor.newInstance(new ZipFileSystemProvider, mleapFilePath, new java.util.HashMap[String, Object])

			// moving to modified OpenJDK ZipFileSystem
			val zfs = new ZipFileSystem(new ZipFileSystemProvider, mleapFilePath, new HashMap[String, Object])

			val mleapPipeline = (for(bf <- managed(BundleFile(zfs, zfs.getPath("/")))) yield {
				bf.loadMleapBundle().get.root
			}).tried.get

			// TODO: also process mleapPipeline.inputSchema to determine the required fields

			mleapPipeline.outputSchema.fields.flatMap {
				mleapField => {
					mleapField.dataType match {
						case _: ScalarType => Some(TypeConverters.mleapFieldToSparkField(mleapField))
						case _ => None
					}
				}
			}
		}
	}
}
