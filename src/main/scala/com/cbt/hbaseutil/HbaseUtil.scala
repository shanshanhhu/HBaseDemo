package com.cbt.hbaseutil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}

/**
 * hbase操作工具类
 */
object HbaseUtil {
  private val config: Configuration = HBaseConfiguration.create()   // 封装hbase的参数

  private val conn: Connection = ConnectionFactory.createConnection(config)   // 构建hbase的连接操作


  private val admin: Admin = conn.getAdmin    // 获取hbase的客户端操作


  def main(args: Array[String]): Unit = {
    // 测试：插入一条数据
    //    putData("test" , "123" , "info" , "tt" , "this is a test")
   // putData("user_table" , "user-003" , "information" , "username" , "jack")
   // putData("user_table" , "user-003" , "information" , "gender" , "male")
    // 测试查询一条数据
    //     val data1 = getData("test", "123" , "info" , "tt")
    //     println(data1)

    // 测试：删除一条数据
    //     deleteData("test", "123", "info")


    // 测试：插入一批数据
    //    var map = Map("t1" -> "123", "t2" -> "234")
    //     putMapData("test", "123", "info", map)
    //val map = Map("phone"->"13617891000","email" ->"123456@qq.com")
   // putMapData("user_table","user-003","contact",map)
    //val map = Map("address"->"tianjin","school" ->"ustc")
    //putMapData("user_table","user-003","contact",map)

    // 测试：查询数据
    //    println(getData("test", "123", "info", "t1"))
    //    println(getData("test", "123", "info", "t2"))
    println(getData("user_table","user-003","contact","address"))
    // 测试：删除一条数据
    //    deleteData("test", "123", "info")
  }


  /**
   * 创建表
   * @param columnFamily 列蔟名
   * @return HBase表
   */
  def init(tableNameStr: String, columnFamily: String): Table = {
    val tableName: TableName = TableName.valueOf(tableNameStr)

    // 构建表的描述器
    val hTableDescriptor = new HTableDescriptor(tableName)
    // 构建列族的描述器
    val hColumnDescriptor = new HColumnDescriptor(columnFamily)

    hTableDescriptor.addFamily(hColumnDescriptor)
    // 如果表不存在则创建表
    if (!admin.tableExists(tableName)) {
      admin.createTable(hTableDescriptor)
    }

    conn.getTable(tableName)

  }


  /**
   * 根据rowkey,列名查询数据
   * @param rowkey rowkey
   * @param columnFamily 列蔟名
   * @param column 列名
   * @return 数据
   */
  def getData(tableNameStr: String, rowkey: String, columnFamily: String, column: String): String = {
    val table: Table = init(tableNameStr, columnFamily)
    var tmp = ""

    try {
      val bytesRowkey = Bytes.toBytes(rowkey)
      val get: Get = new Get(bytesRowkey)
      val result: Result = table.get(get)
      val values: Array[Byte] = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(column.toString))
      if (values != null && values.size > 0) {
        tmp = Bytes.toString(values)
      }else {
        println("你查找的数据不存在，请仔细核对参数-表名,rowkey,列簇,列名")
      }

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      table.close()
    }
    tmp
  }

  /**
   * 批量获取列的数据
   * @param tableNameStr 表名
   * @param rowkey rowkey
   * @param columnFamily 列蔟
   * @param columnList 列的名字列表
   * @return
   */
  def getData(tableNameStr: String, rowkey: String, columnFamily: String, columnList:List[String]) = {
    val table: Table = init(tableNameStr, columnFamily)
    var tmp = ""

    try {
      val bytesRowkey = Bytes.toBytes(rowkey)
      val get: Get = new Get(bytesRowkey)
      val result: Result = table.get(get)
      val valueMap = collection.mutable.Map[String, String]()

      columnList.map{
        col=>
          val values: Array[Byte] = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(col))

          if (values != null && values.size > 0) {
            col -> Bytes.toString(values)
          }
          else {
            ""->""
          }
      }.filter(_._1 != "").toMap
    } catch {
      case e: Exception =>
        e.printStackTrace()
        Map[String, String]()
    } finally {
      table.close()
    }
  }

  /**
   * 插入/更新一条数据
   * @param rowKey rowkey
   * @param columnFamily 列蔟
   * @param column 列名
   * @param data 数据
   */
  def putData(tableNameStr: String, rowKey: String, columnFamily: String, column: String, data: String) = {

    val table: Table = init(tableNameStr, columnFamily)

    try {
      val put: Put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column.toString), Bytes.toBytes(data.toString))
      table.put(put)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      table.close()
    }
  }

  /**
   * 使用Map封装数据，插入/更新一批数据
   * @param rowKey rowkey
   * @param columnFamily 列蔟
   * @param mapData key:列名，value：列值
   */
  def putMapData(tableNameStr: String, rowKey: String, columnFamily: String, mapData: Map[String, String]) = {
    val table: Table = init(tableNameStr, columnFamily)
    try {
      val put: Put = new Put(Bytes.toBytes(rowKey))
      if (mapData.size > 0) {
        for ((k, v) <- mapData) {
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(k), Bytes.toBytes(v))
        }
      }
      table.put(put)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      table.close()
    }
  }

  /**
   * 根据rowkey删除一条数据
   *
   * @param tableNameStr 表名
   * @param rowkey rowkey
   */
  def deleteData(tableNameStr:String, rowkey:String, columnFamily:String) = {
    val tableName: TableName = TableName.valueOf(tableNameStr)

    // 构建表的描述器
    val hTableDescriptor = new HTableDescriptor(tableName)
    // 构建列族的描述器
    val hColumnDescriptor = new HColumnDescriptor(columnFamily)

    hTableDescriptor.addFamily(hColumnDescriptor)
    // 如果表不存在则创建表
    if (!admin.tableExists(tableName)) {
      admin.createTable(hTableDescriptor)
    }

    val table = conn.getTable(tableName)

    try {
      val delete = new Delete(Bytes.toBytes(rowkey))
      table.delete(delete)
    }
    catch {
      case e:Exception => e.printStackTrace()
    }
    finally {
      table.close()
    }
  }




}
