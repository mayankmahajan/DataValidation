import subprocess
DB='perf_db_feb25'
TS1='1451602800'
TS2='1451602801'
FH=open('results.csv','w')
table='f_nrmca_60min_3600_siteflowdatacube'
### Enter the Dimensions
Dimensions=['sourcesiteid', 'sourcesitetypeid', 'sourcesiteelementid', 'pathhash','flowpathhash','nfnameid', 'destsiteid','destsitetypeid','destsiteelementtypeid','isipv6','vrfid','timestamp','downlinkbytebuffer','uplinkbytebuffer','uplinkflowbuffer','downlinkflowbuffer','uplinkcostbuffer','downlinkcostbuffer','transitbuffer','flowpathbuffer']

outer_query=Dimensions[0]
inner_query=Dimensions[0]
groupby=Dimensions[0]
for i in Dimensions[1:]:
    if 'transitbuffer' in i or 'flowpathbuffer' in i:
        outer_query=outer_query+","+i
        inner_query=inner_query+','+'hex(transitSiteUDAF('+i+'))'+' as '+i+' '
    elif 'buffer' in i:
        outer_query=outer_query+','+'DenseVectorUDF('+i+')'
        inner_query=inner_query+','+'genericUDAF('+i+')'+' as '+i+' '
    else:
        outer_query=outer_query+','+ i
        inner_query=inner_query+','+ i
        groupby=groupby+','+i
clause=" where timestamp >= "+TS1+" and timestamp < "+TS2+" "
cmd="echo \"CREATE TEMPORARY FUNCTION transitSiteUDAF as 'com.guavus.nrmca.udaf.TransitSiteUDAF'; CREATE TEMPORARY FUNCTION DenseVectorUDF as 'com.guavus.densevectorudf.DenseVectorUDF'; CREATE TEMPORARY FUNCTION peakUDF as 'com.guavus.densevectorudf.PeakDenseVectorUDF'; CREATE TEMPORARY FUNCTION genericUDAF as 'com.guavus.densevectorudf.GenericDenseVectorUDAFResolver'; CREATE TEMPORARY FUNCTION averageUDF as 'com.guavus.densevectorudf.AverageDenseVectorUDF';use "+DB+";select "+outer_query+" from (select "+inner_query+" from " + table +" "+clause+" group by "+groupby+") T;\" |/opt/spark/bin/spark-sql --jars /opt/tms/java/NrmcaApp.jar,/opt/tms/java/densevectorudf-nrmca3.0.jar,/opt/tms/java/attval-nrmca3.0.jar,/opt/tms/java/attvaludf-nrmca3.0.jar,/opt/tms/ib-framework/editservice/editservice-war/WEB-INF/lib/java-ipv6-0.13.jar,/data/instances/acume/1/app/WEB-INF/lib/nrmca-acume-nrmca3.0.jar  --master yarn  --properties-file /opt/spark/conf/spark-defaults.conf --driver-memory 20G --executor-memory 20G --executor-cores 3 --num-executors 14"
output,error = subprocess.Popen(cmd, shell=True, executable="/bin/bash", stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
FH.write(output+'\n')
FH.close()