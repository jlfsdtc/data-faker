
package com.dunnhumby.datafaker.schema.table.columns

import java.sql.{Date, Timestamp}
import org.scalatest.{MustMatchers, WordSpec}

class SchemaColumnFixedTest extends WordSpec with MustMatchers {

  import com.dunnhumby.datafaker.schema.table.columns.SchemaColumnFixedProtocol._
  import net.jcazevedo.moultingyaml._

  val name = "test"
  val column_type = "Fixed"

  val baseString =
    s"""name: $name
       |column_type: $column_type
    """.stripMargin

  "SchemaColumnFixed" must {
    "read an Int column" in {
      val string =
        s"""$baseString
           |data_type: ${SchemaColumnDataType.Int}
           |value: 1
         """.stripMargin

      string.parseYaml.convertTo[SchemaColumnFixed[_]] mustBe SchemaColumnFixed(name, 1, None)
    }

    "read a Long column" in {
      val string =
        s"""$baseString
           |data_type: ${SchemaColumnDataType.Long}
           |value: 1
         """.stripMargin

      string.parseYaml.convertTo[SchemaColumnFixed[_]] mustBe SchemaColumnFixed(name, 1l, None)
    }

    "read a Float column" in {
      val string =
        s"""$baseString
           |data_type: ${SchemaColumnDataType.Float}
           |value: 1.0
         """.stripMargin

      string.parseYaml.convertTo[SchemaColumnFixed[_]] mustBe SchemaColumnFixed(name, 1f, None)
    }

    "read a Double column" in {
      val string =
        s"""$baseString
           |data_type: ${SchemaColumnDataType.Double}
           |value: 1.0
         """.stripMargin

      string.parseYaml.convertTo[SchemaColumnFixed[_]] mustBe SchemaColumnFixed(name, 1d, None)
    }

    "read a Date column" in {
      val string =
        s"""$baseString
           |data_type: ${SchemaColumnDataType.Date}
           |value: 1998-06-03
         """.stripMargin

      string.parseYaml.convertTo[SchemaColumnFixed[_]] mustBe SchemaColumnFixed(name, Date.valueOf("1998-06-03"), None)
    }

    "read a Timestamp column" in {
      val string =
        s"""$baseString
           |data_type: ${SchemaColumnDataType.Timestamp}
           |value: 1998-06-03 01:23:45
         """.stripMargin

      string.parseYaml.convertTo[SchemaColumnFixed[_]] mustBe SchemaColumnFixed(name, Timestamp.valueOf("1998-06-03 01:23:45"), None)
    }

    "read a String column" in {
      val string =
        s"""$baseString
           |data_type: ${SchemaColumnDataType.String}
           |value: test
         """.stripMargin

      string.parseYaml.convertTo[SchemaColumnFixed[_]] mustBe SchemaColumnFixed(name, "test", None)
    }

    "read a Boolean column" in {
      val string =
        s"""$baseString
           |data_type: ${SchemaColumnDataType.Boolean}
           |value: true
         """.stripMargin

      string.parseYaml.convertTo[SchemaColumnFixed[_]] mustBe SchemaColumnFixed(name, true, None)
    }
  }
}
