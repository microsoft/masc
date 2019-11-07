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

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import com.google.common.io.Resources
import java.util.Base64

import org.apache.spark.sql.types.{DataTypes, StructField}

@RunWith(classOf[JUnitRunner])
class VerifyMleapSchema extends FunSuite {
  test("Validate mleap schema extraction") {
    val mleapPath = classOf[VerifyMleapSchema].getResource("sentiment.zip").toString

  	val fields = MLeapUtil.mleapSchemaToCatalyst(mleapPath)

    assert(Seq(StructField("prediction", DataTypes.DoubleType, false)) ==  fields)
  }
}