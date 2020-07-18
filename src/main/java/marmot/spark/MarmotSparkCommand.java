package marmot.spark;

import org.apache.spark.sql.SparkSession;

import marmot.hadoop.MarmotHadoopServer;
import marmot.hadoop.command.MarmotHadoopCommand;
import picocli.CommandLine.Option;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class MarmotSparkCommand extends MarmotHadoopCommand {
	@Option(names={"-nthreads"}, paramLabel="count", description={"the number of threads for local configuration"})
	private int m_localThreadCount = 3;
	
	@Option(names={"-lock"}, paramLabel="path", description={"MarmotServer termination-lock file"})
	private String m_lock = null;
	
	protected abstract void run(MarmotSpark marmot) throws Exception;

	@Override
	protected final void run(MarmotHadoopServer server) throws Exception {
		SparkSession spark = SparkSession.builder()
										.appName("marmot_spark_server")
										.master("local[" + m_localThreadCount + "]")
										.config("spark.driver.host", "localhost")
										.config("spark.driver.maxResultSize", "5g")
										.config("spark.executor.memory", "5g")
										.getOrCreate();
		MarmotSpark marmot = new MarmotSpark(server, spark);
		run(marmot);
	}
}
