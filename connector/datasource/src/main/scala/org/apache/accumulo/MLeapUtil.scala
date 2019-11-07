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

package org.apache.accumulo

import org.apache.spark.sql.types.StructField
import ml.combust.bundle.BundleFile
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.mleap.runtime.MleapContext.defaultContext
import org.apache.spark.sql.mleap.TypeConverters
import java.io.File
import java.util.Base64
import java.net.URI
import resource._
import ml.combust.mleap.core.types.ScalarType
// https://github.com/marschall/memoryfilesystem has a 16MB file size limitation
import com.google.common.jimfs.{Jimfs, Configuration}
import java.nio.file.{Files, FileSystem, FileSystems, Path, StandardOpenOption}

@SerialVersionUID(1L)
object MLeapUtil {

	// public static FileSystem fileSystemForZip(final Path pathToZip) { 
	//Objects.requireNotNull(pathToZip, "pathToZip is null"); try { return FileSystems.getFileSystem(pathToZipFile.toUri()); } catch (Exception e) {
	//	 try { return FileSystems.getFileSystem(URI.create("jar:" + pathToZipFile.toUri())); } catch (Exception e2) {
	//		  return FileSystems.newFileSystem( URI.create("jar:" + pathToZipFile.toUri()), new HashMap<>()); } } }

	// load the Spark pipeline we saved in the previous section
	def mleapSchemaToCatalyst(modelBase64: String): Seq[StructField] = {
		if (modelBase64.isEmpty)
			Seq.empty[StructField]
		else {
			val mleapBundleArr = Base64.getDecoder().decode(modelBase64)

			val fs = Jimfs.newFileSystem(Configuration.unix())
			val mleapFilePath = fs.getPath("/mleap.zip")
			Files.write(mleapFilePath, mleapBundleArr, StandardOpenOption.CREATE)

			// https://commons.apache.org/proper/commons-vfs/filesystems.html#ram
			println(s"MLEAP in memory file path: ${mleapFilePath.toUri}")

			// create a zip file system view into the zip
			val zfs = FileSystems.newFileSystem(URI.create("jar:" +  mleapFilePath.toUri), new java.util.HashMap[String, Object]())
    
			val mleapPipeline = (for(bf <- managed(BundleFile(zfs, zfs.getPath("/")))) yield {
				bf.loadMleapBundle().get.root
			}).tried.get

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