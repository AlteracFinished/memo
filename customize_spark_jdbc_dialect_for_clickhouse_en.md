# Customize Spark JDBC dialect for ClickHouse

## PROBLEM DESCRIPTION

Currently, Spark supports ClickHouse JDBC Data Source. However, we would get in trouble inserting data with Array or Datetime64 to ClickHouse. The following Scala code shows how to fix it.

## Implementation

```scala
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.types._
 
 
object ClickhouseDialect extends JdbcDialect {
 
  override def canHandle(url: String): Boolean = url.startsWith("jdbc:clickhouse")
 
  override def getCatalystType(sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    if (typeName.contains("Array(String)")) {
      Option(ArrayType(StringType, false))
    } else if (typeName.contains("Array(Long)")) {
      Option(ArrayType(LongType, false))
    } else if(typeName.contains("DateTime64")){
      Option(TimestampType)
    } else {
      None
    }
  }
   
  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case TimestampType => Some(JdbcType("DateTime64", java.sql.Types.TIMESTAMP))
    case ArrayType(et, _) if et.isInstanceOf[AtomicType] => getJDBCType(et).orElse(JdbcUtils.getCommonJDBCType(et))
        .map(jdbcType => JdbcType(s"Array(${jdbcType.databaseTypeDefinition})", java.sql.Types.ARRAY))
    case _ => None
  }
   
  override def compileValue(value: Any): Any = value match {
    case arrayValue: Array[Any] => arrayValue.map(compileValue).mkString("Array(", ",", ")")
    case _ => value
  }
}
 
JdbcDialects.registerDialect(ClickhouseDialect)
```

## REFERENCE

1. [Customize Spark JDBC Data Source to work with your dedicated Database Dialect](https://medium.com/@huaxingao/customize-spark-jdbc-data-source-to-work-with-your-dedicated-database-dialect-beec6519af27)