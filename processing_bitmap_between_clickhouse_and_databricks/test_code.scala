import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.io.DataOutputStream
import java.io.ByteArrayOutputStream
import org.roaringbitmap.longlong.Roaring64NavigableMap
import org.roaringbitmap.BitmapDataProvider
import io.opencensus.implcore.internal.VarInt
import java.util.TreeMap
import java.util.UUID
import java.util.Base64
import java.util.Properties

def toBase64Str(bf: ByteBuffer): String = {
  new String(Base64.getEncoder.encode(bf.array()))
}

def serialize(rb: Roaring64NavigableMap): String = {
    // ck中rbm对小于32的基数进行了优化，使用smallset进行存放
    if (rb.getLongCardinality <= 32) {
      // the serialization structure of roaringbitmap in clickhouse: Byte(1), VarInt(SerializedSizeInBytes), ByteArray(RoaringBitmap)
      // and long occupies 8 bytes
      val length = 1 + 1 + 8 * rb.getIntCardinality
      val bos1 = ByteBuffer.allocate(length)
      val bos = if (bos1.order eq ByteOrder.LITTLE_ENDIAN) bos1 else bos1.slice.order(ByteOrder.LITTLE_ENDIAN)
      bos.put(new Integer(0).toByte)
      bos.put(rb.getIntCardinality.toByte)
      rb.toArray.foreach(i => bos.putLong(i))
      toBase64Str(bos)
    } else {
      // Roaring64NavigableMap serialize with prefix of "signedLongs" and "highToBitmap.size()"
      // Refer to the implementation of the serialize method of Roaring64NavigableMap, remove the prefix bytes
      // bos: Byte(1) + VarInt(rbTotalSize) + highToBitmap.size() + baos.toByteArray.slice(rbmPrefixBytes, serializedSizeInBytes)
      val rbmPrefixBytes = 1 + 4
      val serializedSizeInBytes = rb.serializedSizeInBytes().toInt
      val rbTotalSize = serializedSizeInBytes - rbmPrefixBytes + 8
      val varIntLen = VarInt.varLongSize(rbTotalSize)
      val length = 1 + varIntLen + rbTotalSize
      // the serialization structure of roaringbitmap in clickhouse: Byte(1), VarInt(SerializedSizeInBytes), ByteArray(RoaringBitmap)
      val bos1 = ByteBuffer.allocate(length)
      val bos = if (bos1.order eq ByteOrder.LITTLE_ENDIAN) bos1 else bos1.slice.order(ByteOrder.LITTLE_ENDIAN)
      bos.put(new Integer(1).toByte)
      VarInt.putVarInt(rbTotalSize, bos)
      val baos = new ByteArrayOutputStream()
      val rbClass = classOf[Roaring64NavigableMap]
      val field = rbClass.getDeclaredField("highToBitmap")
      field.setAccessible(true)
//       val highToBitmap = rb.getHighToBitmap()
      val highToBitmap = field.get(rb).asInstanceOf[TreeMap[Integer, BitmapDataProvider]]
      bos.putLong(highToBitmap.size())
      rb.serialize(new DataOutputStream(baos))
      bos.put(baos.toByteArray.slice(rbmPrefixBytes, serializedSizeInBytes))
      toBase64Str(bos)
    }
  }

val arrayToBase64EncodedStr = (obj: Seq[Long]) => {
  val bitmap = new Roaring64NavigableMap()
  for (item <- obj) {
    bitmap.addLong(item)
  }
  serialize(bitmap)
}

spark.udf.register("array_to_base64encoded_str", arrayToBase64EncodedStr)

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.io.DataOutputStream
import java.io.ByteArrayOutputStream
import org.roaringbitmap.longlong.Roaring64NavigableMap
import org.roaringbitmap.BitmapDataProvider
import io.opencensus.implcore.internal.VarInt
import java.util.TreeMap

  

import java.sql.Statement
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import io.delta.tables._
import org.apache.spark.sql.functions._
import spark.implicits._
import java.util.Base64
import java.nio.charset.StandardCharsets
import java.io.ByteArrayInputStream
import java.io.DataInputStream
import scala.math.BigInt
import scala.util.Try

def deserialize(input: String): Roaring64NavigableMap = {
  val byteArr = Base64.getDecoder().decode(input)
  val state = byteArr(0).intValue
  if (state == 0) {
    val bitmap = new Roaring64NavigableMap()
    val size = byteArr(1).intValue
    val arrSize = byteArr.size
    val byteBuffer = ByteBuffer.wrap(byteArr.slice(2, arrSize)).order(ByteOrder.LITTLE_ENDIAN)
    for (idx <- 1 to size) {
      bitmap.addLong(byteBuffer.getLong())
    }
    return bitmap
  } else {
    val intDest = Array.fill[Int](1)(0)
    val varInt = VarInt.getVarInt(byteArr, 1, intDest)
    val arrSize = byteArr.size
    val rbTotalSize = intDest(0)
    val varIntLen = VarInt.varIntSize(rbTotalSize)
    val bitmapStart = 1 + varIntLen + 8
    val byteArrOrder = ByteBuffer.wrap(byteArr.slice(1 + varIntLen, 1 + varIntLen + 8)).order(ByteOrder.LITTLE_ENDIAN)
    val highToBitmapSizeBytes = BigInt(byteArrOrder.getLong()).toByteArray.reverse.padTo(4,0).reverse.map( x => x.asInstanceOf[Number].byteValue )
    val bitmap = new Roaring64NavigableMap()
    val nextArr = Array[Byte](1) ++ highToBitmapSizeBytes ++ byteArr.slice(bitmapStart, arrSize)
    // val nextArr = Array[Byte](1) ++ byteArr.slice(1, arrSize)
    // val nextArrSize = byteArr.slice(bitmapStart, arrSize).size
    val bais = new ByteArrayInputStream(nextArr)
    bitmap.deserialize(new DataInputStream(bais))
    return bitmap
  }
}

val strToBase64DecodedArray = (obj: String) => {
  val bitmap = deserialize(obj)
  bitmap.toArray()
}

spark.udf.register("str_to_base64decoded_array", strToBase64DecodedArray)

// part 1: process detail table in ck, read detail table, process union, write detail table to clickhouse, generate bitmap
val sourceTagId = 1
val destTagId = 11

def oldApproach(sourceTagId: Long, destTagId: Long) {

val jdbcUrl = s"jdbc:clickhouse://${ckHost}:${ckPort}/default?user=${ckUser}&password=${ckPass}"

Class.forName("ru.yandex.clickhouse.ClickHouseDriver")

val connection: Connection = DriverManager.getConnection(jdbcUrl, ckUser, ckPass)
val statement: Statement = connection.createStatement()

val cleanSourceTagTableQuery = s"""
    truncate table tag_${sourceTagId}_customer_relation_local ON CLUSTER `cdp_ck`
"""
statement.executeQuery(cleanSourceTagTableQuery)

val insertSourceTagTableQuery = s"""
    insert into tag_${sourceTagId}_customer_relation_distributed
    select ${sourceTagId}, arrayJoin(bitmapToArray(customer_ids)) from tag_customer_relation_distributed where tag_id = ${sourceTagId};
"""
statement.executeQuery(insertSourceTagTableQuery)

val sourceTagMemberIds = spark.read.format("jdbc")
  .option("url", jdbcUrl)
  .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
  .option("dbtable", s"tag_${sourceTagId}_customer_relation_distributed")
  .option("user", ckUser)
  .option("password", ckPass)
  .load().collect().map(_.getLong(1)).toList

val sourceTagDF = spark.createDataFrame(Seq((sourceTagId.toLong, sourceTagMemberIds)))
     .toDF("tag_id", "customer_ids")

sourceTagDF.write.mode("append").format("delta").saveAsTable("mh_tag.tag_customer_relation")

val destTagSQL = s"""
insert into mh_tag.tag_customer_relation
select ${destTagId}, array_union(customer_ids, (select collect_list(id) from ods.member where id % 2 = 1)) from mh_tag.tag_customer_relation where tag_id = ${sourceTagId}
"""

spark.sql(destTagSQL)

val destDetailTruncateSQL = s"""
truncate table mh_tag.tag_11_customer_relation
"""

spark.sql(destDetailTruncateSQL)

val destDetailSQL = s"""
insert into mh_tag.tag_11_customer_relation
select ${destTagId}, explode(customer_ids) from mh_tag.tag_customer_relation where tag_id = ${destTagId}
"""

spark.sql(destDetailSQL)

val destTagRelationToCK = spark.sql("select * from mh_tag.tag_11_customer_relation")

destTagRelationToCK.write.mode("append").format("jdbc")
  .option("url", jdbcUrl)
  .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
  .option("user", ckUser)
  .option("password", ckPass)
  .option("dbtable", s"tag_${destTagId}_customer_relation_distributed")
  .save()

val insertSourceTagTablToCKQuery = s"""
    insert into tag_customer_relation_distributed (tag_id, customer_ids)
    select ${destTagId}, groupBitmapState(customer_id) from tag_${destTagId}_customer_relation_distributed;
"""
statement.executeQuery(insertSourceTagTablToCKQuery)
}

oldApproach(1, 11)

// part 2: read bitmap, process union, write bitmap str
val sourceTagId = 2
val destTagId = 12

Class.forName("ru.yandex.clickhouse.ClickHouseDriver")

val jdbcUrl = s"jdbc:clickhouse://${ckHost}:${ckPort}/default?user=${ckUser}&password=${ckPass}"

val connection: Connection = DriverManager.getConnection(jdbcUrl, ckUser, ckPass)
val statement: Statement = connection.createStatement()

val encodedCustomerIdsQuery = s"""
select tag_id, base64Encode(toString(customer_ids)) from tag_customer_relation_distributed where tag_id = ${sourceTagId}
"""

val encodedCustomerIdStr = spark.read.format("jdbc")
  .option("url", jdbcUrl)
  .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
  .option("query", encodedCustomerIdsQuery)
  .option("user", ckUser)
  .option("password", ckPass)
  .load()
  .collect().map(_.getString(1)).toList.head

val sourceTagDF = spark.createDataFrame(Seq((sourceTagId.toLong, deserialize(encodedCustomerIdStr).toArray.toList)))
     .toDF("tag_id", "customer_ids")

sourceTagDF.write.mode("append").format("delta").saveAsTable("mh_tag.tag_customer_relation")

val destTagSQL = s"""
insert into mh_tag.tag_customer_relation
select ${destTagId}, array_union(customer_ids, (select collect_list(id) from ods.member where id % 2 = 1)) from mh_tag.tag_customer_relation where tag_id = ${sourceTagId}
"""

spark.sql(destTagSQL)

val destEncodedStrSQL = s"""
select ${destTagId} as tag_id, array_to_base64encoded_str(customer_ids) as customer_id_str from mh_tag.tag_customer_relation where tag_id = ${destTagId}
"""

val destEncodedStrDF = spark.sql(destEncodedStrSQL)

destEncodedStrDF.write.mode("append").format("jdbc")
  .option("url", jdbcUrl)
  .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
  .option("dbtable", s"tag_customer_relation_distributed_temp")
  .option("user", ckUser)
  .option("password", ckPass)
  .save()

val insertSourceTagTableQuery = s"""
    insert into tag_customer_relation_distributed (tag_id, customer_ids)
    select tag_id, customer_ids from tag_customer_relation_distributed_temp where tag_id = ${destTagId};
"""
statement.executeQuery(insertSourceTagTableQuery)


