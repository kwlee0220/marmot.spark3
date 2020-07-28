package marmot.spark;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.UDFRegistration;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.UDTRegistration;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import marmot.MarmotRuntime;
import marmot.RecordSchema;
import marmot.dataset.AbstractDataSetServer;
import marmot.dataset.DataSet;
import marmot.dataset.DataSetInfo;
import marmot.dataset.DataSetServer;
import marmot.dataset.DataSetType;
import marmot.file.FileServer;
import marmot.spark.type.EnvelopeUDT;
import marmot.spark.type.GeometryCollectionUDT;
import marmot.spark.type.GeometryUDT;
import marmot.spark.type.LineStringUDT;
import marmot.spark.type.MultiLineStringUDT;
import marmot.spark.type.MultiPointUDT;
import marmot.spark.type.MultiPolygonUDT;
import marmot.spark.type.PointUDT;
import marmot.spark.type.PolygonUDT;
import utils.UnitUtils;
import utils.func.Tuple;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotSpark implements MarmotRuntime, Serializable {
	private static final long serialVersionUID = 1L;
	
	private static final long DEF_TEXT_BLOCK_SIZE = UnitUtils.parseByteSize("128m");
	private static final long DEF_BLOCK_SIZE = UnitUtils.parseByteSize("64m");
	private static final long DEF_CLUSTER_SIZE = UnitUtils.parseByteSize("64m");
	private static final int DEF_PARTITIOIN_COUNT = 7;
	
	private final MarmotRuntime m_marmot;
	private final SparkSession m_spark;
	private transient JavaSparkContext m_jsc;
	
	public static MarmotSpark forLocal(MarmotRuntime marmot) throws Exception {
		SparkSession spark = SparkSession.builder()
										.appName("marmot_spark_server")
										.master("local[3]")
										.config("spark.driver.host", "localhost")
										.config("spark.driver.maxResultSize", "5g")
										.config("spark.executor.memory", "5g")
										.getOrCreate();
		return new MarmotSpark(marmot, spark);
	}

	public MarmotSpark(MarmotRuntime marmot, SparkSession spark) {
		m_marmot = marmot;
		m_spark = spark;
		
		registerUdts();
		registerUdfs(m_spark.sqlContext().udf());

		m_jsc = JavaSparkContext.fromSparkContext(m_spark.sparkContext());
		m_jsc.hadoopConfiguration().set("avro.mapred.ignore.inputs.without.extension", "false");
	}

	@Override
	public DataSetServer getDataSetServer() {
		return m_marmot.getDataSetServer();
	}

	@Override
	public FileServer getFileServer() {
		return m_marmot.getFileServer();
	}
	
	public SparkSession getSparkSession() {
		return m_spark;
	}
	
	public SQLContext getSqlContext() {
		return m_spark.sqlContext();
	}
	
	public JavaSparkContext getJavaSparkContext() {
		return m_jsc;
	}
	
	public Tuple<RecordSchema, Dataset<Row>> readDataSet(String id) {
		AbstractDataSetServer server = (AbstractDataSetServer)m_marmot.getDataSetServer();
		RecordSchema schema = server.getDataSet(id).getRecordSchema();
		
		String uri = server.getDataSetUri(id);
		Dataset<Row> rows = m_spark.read().format("com.databricks.spark.avro").load(uri);
		
		for ( marmot.Column col: schema.streamColumns().filter(c -> c.type().isGeometryType()) ) {
			Column sqlCol = functions.col(col.name());
			if ( col.type().isGeometryType() ) {
				rows = rows.withColumn(col.name(), functions.callUDF("ST_GeomFromWKB", sqlCol));
			}
			else {
				switch ( col.type().typeClass() ) {
					case DATETIME:
						rows = rows.withColumn(col.name(), sqlCol.cast("long"))
									.withColumn(col.name(), functions.from_unixtime(sqlCol));
						break;
					default: break;
				}
			}
		}
		
		return Tuple.of(schema, rows);
	}
	
	public void writeDataSet(String dsId, RecordSchema schema, Dataset<Row> rows) {
		AbstractDataSetServer server = (AbstractDataSetServer)m_marmot.getDataSetServer();
		
		for ( marmot.Column col: schema.streamColumns() ) {
			Column sqlCol = functions.col(col.name());
			if ( col.type().isGeometryType() ) {
				rows = rows.withColumn(col.name(), functions.callUDF("ST_AsBinary", sqlCol));
			}
			else {
				switch ( col.type().typeClass() ) {
					case DATETIME:
						rows = rows.withColumn(col.name(), sqlCol.cast("long"));
						break;
					default: break;
				}
			}
		}
		
		DataSetInfo info = new DataSetInfo(dsId, DataSetType.AVRO, schema);
		DataSet ds = server.createDataSet(info, true);
		String uri = server.getDataSetUri(dsId);
		rows.write().mode(SaveMode.Append).format("com.databricks.spark.avro").save(uri);
	}
	
	public Dataset<Row> readAvroFile(String path) {
		return m_spark.read().format("com.databricks.spark.avro").load(path);
	}
	
	public Dataset<Row> readCsvFile(Map<String,String> options, String... path) {
		return m_spark.read()
						.options(options)
						.csv(path);
						
	}
	
	private static void registerUdts() {
		UDTRegistration.register(Envelope.class.getName(), EnvelopeUDT.class.getName());
		
		UDTRegistration.register(Point.class.getName(), PointUDT.class.getName());
		UDTRegistration.register(MultiPoint.class.getName(), MultiPointUDT.class.getName());
		UDTRegistration.register(Polygon.class.getName(), PolygonUDT.class.getName());
		UDTRegistration.register(MultiPolygon.class.getName(), MultiPolygonUDT.class.getName());
		UDTRegistration.register(LineString.class.getName(), LineStringUDT.class.getName());
		UDTRegistration.register(MultiLineString.class.getName(), MultiLineStringUDT.class.getName());
		UDTRegistration.register(GeometryCollection.class.getName(), GeometryCollectionUDT.class.getName());
		UDTRegistration.register(Geometry.class.getName(), GeometryUDT.class.getName());
	}
	
	private static void registerUdfs(UDFRegistration registry) {
		SpatialUDFs.registerUdf(registry);
	}
	
	private Object writeReplace() {
		return new SerializationProxy(this);
	}
	
	private void readObject(ObjectInputStream stream) throws InvalidObjectException {
		throw new InvalidObjectException("Use Serialization Proxy instead.");
	}

	private static class SerializationProxy implements Serializable {
		private static final long serialVersionUID = 1L;
		private final byte[] m_serializedBytes;
		
		private SerializationProxy(MarmotSpark mspark) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			try ( ObjectOutputStream out = new ObjectOutputStream(baos) ) {
				out.writeObject(mspark.m_marmot);
				out.writeObject(mspark.m_spark);
			}
			catch ( IOException nerverHappens ) { }
			m_serializedBytes = baos.toByteArray();
		}
		
		private Object readResolve() {
			try ( ObjectInputStream dis = new ObjectInputStream(new ByteArrayInputStream(m_serializedBytes)) ) {
				MarmotRuntime marmot = (MarmotRuntime)dis.readObject();
				SparkSession session = (SparkSession)dis.readObject();
				
				return new MarmotSpark(marmot, session);
			}
			catch ( Exception e ) {
				throw new RuntimeException(e);
			}
		}
	}
}
