sqlContext.sql("Select SourceSiteId,SourceSiteTypeId,DestSiteId,DestSiteTypeId from p1 where SourceSiteTypeId=1 and SourceSiteId=10 and DestSiteTypeId=1").show



var sitedatacube = sqlContext.parquetFile("/user/hive/warehouse/nrmca41d2.db/f_nrmca_60min_3600_sitedatacube/exporttimestamp=1458612000/timestamp=1458612000/*","/user/hive/warehouse/nrmca41d2.db/f_nrmca_60min_3600_sitedatacube/exporttimestamp=1458615600/timestamp=1458615600/*")


/user/hive/warehouse/nrmca41d2.db/f_nrmca_60min_3600_sitedatacube/exporttimestamp=1458615600/timestamp=1458615600






    # downlinkByteBuffer1 = '52000C57942120056CBA6CBD6CBB6CBD6CBB6CBB6CB96CBC6CBC6CBC6CBB6CBB'
    # print downlinkByteBuffer[i*4:i*4+4]
import org.apache.spark.sql.functions._


def test_binaryToArrayUDF(byteBuffer: String) : String = {
    # //var downlinkByteBuffer1 = hex(binary)
    # var downlinkByteBuffer = byteBuffer[16:]
    var downlinkByteBuffer = byteBuffer.substring(16)
    # print len(downlinkByteBuffer)
    var arr =Array[Int]()
    for i in range(12):
    for (i <- 0 to 12) {



        arr.append(int(downlinkByteBuffer[i*4:i*4+4],16))
        arr.append(int(downlinkByteBuffer.substring(i*4,i*4+4),16))
        arr.append(Integer.parseInt(downlinkByteBuffer,16))

    return arr.toString



###########################
def test_binaryToArrayUDF(byteBuffer: String) : String = {
      var downlinkByteBuffer = byteBuffer.substring(16)
      var arr =Array[Int]()
      var arrString:String = ""
      for (i <- 0 to 11) {
      var value = Integer.parseInt(downlinkByteBuffer.substring(i*4,i*4+4),16)
      arrString = arrString +"_"+value.toString()
      }
      return arrString}
# test_binaryToArrayUDF: (byteBuffer: String)String

##################################



var parq1 = sqlContext.parquetFile("/user/hive/warehouse/mayankdb2.db/f_nrmca_60min_3600_siteflowdatacube/*/*")

("/user/hive/warehouse/nrmca41d2.db/f_nrmca_60min_3600_sitedatacube/exporttimestamp=1458612000/*,/user/hive/warehouse/nrmca41d2.db/f_nrmca_60min_3600_sitedatacube/exporttimestamp=1458612000/*")

            ("/user/hive/warehouse/nrmca41d2.db/f_nrmca_60min_3600_siteflowdatacube/exporttimestamp=1458612000/*","/user/hive/warehouse/nrmca41d2.db/f_nrmca_60min_3600_siteflowdatacube/exporttimestamp=1458612000/*")
var df = parq1.select('SourceSiteId,'SourceSiteTypeId,(hex(col("DownlinkByteBuffer"))),'timestamp)


scala>  import org.apache.spark.sql._
import org.apache.spark.sql._

scala> import org.apache.spark.sql.types._
import org.apache.spark.sql.types._

    var df1 = df.flatMap(record => {
      var value:String = test_binaryToArrayUDF(record.get(2).toString)
      val res = collection.mutable.MutableList[org.apache.spark.sql.Row]()
        res += Row.fromSeq(Seq[Any](value))
      res
      })

columns = ['flowpathhash','sourcesiteid', 'sourcesitetypeid', 'sourcesiteelementid', 'sourcesiteelementtypeid', 'nfnameid', 'destsiteid', 'destsitetypeid', 'destsiteelementid', 'destsiteelementtypeid', 'isipv6', 'vrfid', 'timestamp','downlinkbytebuffer','uplinkbytebuffer','uplinkflowbuffer','downlinkflowbuffer','uplinkcostbuffer','downlinkcostbuffer','transitbuffer']

var schemaFlow = StructType(Seq(StructField("FIRST_SWITCHED", StringType, nullable = true)))

var schemaFlow = StructType(StructField(SourceSiteId,IntegerType,true), StructField(SourceSiteTypeId,IntegerType,true), StructField(SourceSiteElementId,IntegerType,true), StructField(SourceSiteElementTypeId,IntegerType,true), StructField(NfNameId,IntegerType,true), StructField(DestSiteId,IntegerType,true), StructField(DestSiteTypeId,IntegerType,true), StructField(DestSiteElementId,IntegerType,true), StructField(DestSiteElementTypeId,IntegerType,true), StructField(IsIpv6,IntegerType,true), StructField(VrfId,IntegerType,true), StructField(PathHash,IntegerType,true), StructField(FlowPathHash,IntegerType,true), StructField(TransitBuffer,BinaryType,true), StructField(FlowPathBuffer,BinaryType,true), StructField(UplinkByteBuffer,BinaryType,true))

var df2 = sqlContext.createDataFrame(df1,schemaFlow)