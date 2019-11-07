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
import resource._
import ml.combust.mleap.core.types.ScalarType
// https://github.com/marschall/memoryfilesystem has a 16MB file size limitation
<<<<<<< HEAD
=======
import com.google.common.jimfs.{Jimfs, Configuration}
import java.nio.file.{Files, FileSystem, FileSystems, Path, StandardOpenOption}
>>>>>>> master

@SerialVersionUID(1L)
object MLeapUtil {

	// load the Spark pipeline we saved in the previous section
	def mleapSchemaToCatalyst(modelPath: String): Seq[StructField] = {
		if (modelPath.isEmpty)
			Seq.empty[StructField]
		else {
			val mleapPipeline = (for(bf <- managed(BundleFile.load("jar:" + modelPath))) yield {
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