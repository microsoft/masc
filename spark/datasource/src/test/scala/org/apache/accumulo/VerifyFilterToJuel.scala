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

import java.io.ByteArrayOutputStream

import org.apache.spark.sql.sources._
import org.junit.runner.RunWith

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class VerifyFilterToJuel extends FunSuite {
  val map = Map[String, String](
    "i" -> "i0",
    "x" -> "x",
    "j" -> "j",
    "k" -> "k",
    "x.yZ" -> "xyZ")

  test("Validate filter to juel operators") {
    assert("(i0 == 5)".equals(new FilterToJuel(map).serializeFilter(new EqualTo("i", 5))))
    assert("(i0 >= 5)".equals(new FilterToJuel(map).serializeFilter(new GreaterThanOrEqual("i", 5))))
    assert("(i0 > 5)".equals(new FilterToJuel(map).serializeFilter(new GreaterThan("i", 5))))
    assert("(i0 <= 5)".equals(new FilterToJuel(map).serializeFilter(new LessThanOrEqual("i", 5))))
    assert("(i0 < 5)".equals(new FilterToJuel(map).serializeFilter(new LessThan("i", 5))))
    assert("(i0 == null)".equals(new FilterToJuel(map).serializeFilter(new IsNull("i"))))
    assert("(i0 != null)".equals(new FilterToJuel(map).serializeFilter(new IsNotNull("i"))))
  }

  test("Validate filter to juel composed operators") {
    assert("(!(i0 == 5))".equals(new FilterToJuel(map).serializeFilter(
      new Not(new EqualTo("i", 5)))))

    assert("((i0 == 5) && (x == 3.0))".equals(new FilterToJuel(map).serializeFilter(
      new And(new EqualTo("i", 5), new EqualTo("x", 3.0)))))

    assert("((i0 == 5) || (x == 3.0))".equals(new FilterToJuel(map).serializeFilter(
      new Or(new EqualTo("i", 5), new EqualTo("x", 3.0)))))
  }

  test("Validate filter to juel string operators") {
    assert("x.contains('abc')".equals(new FilterToJuel(map).serializeFilter(
      new StringContains("x", "abc"))))
    assert("x.startsWith('abc')".equals(new FilterToJuel(map).serializeFilter(
      new StringStartsWith("x", "abc"))))
    assert("x.endsWith('abc')".equals(new FilterToJuel(map).serializeFilter(
      new StringEndsWith("x", "abc"))))
  }

  test("Validate filter to juel in operator") {
    assert("xyZ.in('abc','def','ghi')".equals(new FilterToJuel(map).serializeFilter(
      new In("x.yZ", Array("abc", "def", "ghi")))))
  }

  test("Validate filter string escape") {
    assert("(i0 == '\\'')".equals(new FilterToJuel(map).serializeFilter(new EqualTo("i", "'"))))
    assert("(i0 == '\\\\')".equals(new FilterToJuel(map).serializeFilter(new EqualTo("i", "\\"))))
    assert("(i0 == '\\\\\\'')".equals(new FilterToJuel(map).serializeFilter(new EqualTo("i", "\\'"))))
  }

  test("Validate filter combining") {
    val filters = Array[Filter](
      new EqualTo("i", 5),
      new EqualTo("j", 3),
      new EqualTo("k", 4)
    )

    val result = new FilterToJuel(map).serializeFilters(filters, "")

    assert("(i0 == 5) && (j == 3) && (k == 4)".equals(result.serializedFilter))
    assert(filters.length == result.supportedFilters.length)

    assert(result.unsupportedFilters.isEmpty)
  }

  test("Validate filter with rowKey and manual filter") {
    val filters = Array[Filter](
      new EqualTo("i", 5),
      new EqualTo("j", 3),
      new EqualTo("k", 4),
      new EqualTo("rowKey", "foo")
    )

    val result = new FilterToJuel(map).serializeFilters(filters, "a.b == 3")

    assert("(i0 == 5) && (j == 3) && (k == 4) && (rowKey == 'foo') && (a.b == 3)".equals(result.serializedFilter))
    assert(filters.length == result.supportedFilters.length)

    assert(result.unsupportedFilters.isEmpty)
  }
}