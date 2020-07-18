package marmot.spark.type;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.UserDefinedType;

import com.vividsolutions.jts.geom.Point;

import marmot.type.GeometryDataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PointUDT extends UserDefinedType<Point> {
	private static final long serialVersionUID = -1L;

	public static final PointUDT UDT;
	private static final DataType SQL_DATA_TYPE;
	
	static {
		SQL_DATA_TYPE = DataTypes.createStructType(new StructField[] {
			DataTypes.createStructField("wkb", DataTypes.BinaryType, false),
		});
		UDT = new PointUDT();
	}

	@Override
	public DataType sqlType() {
		return SQL_DATA_TYPE;
	}

	@Override
	public InternalRow serialize(Point pt) {
		InternalRow row = new GenericInternalRow(1);
		row.update(0, GeometryDataType.toWkb(pt));
		
		return row;
	}

	@Override
	public Point deserialize(Object datum) {
		if ( datum instanceof InternalRow ) {
			InternalRow row = (InternalRow)datum;
			byte[] wkb = row.getBinary(0);
			return (Point)GeometryDataType.fromWkb(wkb);
		}

		throw new IllegalArgumentException("datum is not Row: " + datum);
	}

	@Override
	public Class<Point> userClass() {
		return Point.class;
	}

}
