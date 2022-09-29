package exercise_4;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;

public class Exercise_4 {

	private static final URL EDGES_RESOURCE = Exercise_4.class.getClassLoader().getResource("wiki-edges.txt");
	private static final URL VERTICES_RESOURCE = Exercise_4.class.getClassLoader().getResource("wiki-vertices.txt");
	private static final double DAMPING_FACTOR = 0.85;
	private static final int MAX_ITERATIONS = 10;

	public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) throws Exception {
		Dataset<Row> vertices = loadVertices(ctx, sqlCtx);
		Dataset<Row> edges = loadEdges(ctx, sqlCtx);

		GraphFrame graph = GraphFrame.apply(vertices, edges);
		GraphFrame results = graph.pageRank()
				.resetProbability(1 - DAMPING_FACTOR)
				.maxIter(MAX_ITERATIONS)
				.run();
		results.vertices().select("id", "title", "pagerank")
				.orderBy(new Column("pageRank").desc())
				.limit(10)
				.show();
	}

	private static Dataset<Row> loadVertices(JavaSparkContext ctx, SQLContext sqlCtx) throws IOException, URISyntaxException {
		List<Row> vertices = new ArrayList<>();
		File file = new File(VERTICES_RESOURCE.toURI());
		BufferedReader br = new BufferedReader(new FileReader(file));
		String st;
		while ((st = br.readLine()) != null) {
			String[] split = st.split("\t");
			long id = Long.parseLong(split[0]);
			String name = split[1];
			vertices.add(RowFactory.create(id, name));
		}

		JavaRDD<Row> vertices_rdd = ctx.parallelize(vertices);
		StructType vertices_schema = new StructType(new StructField[]{
				new StructField("id", DataTypes.LongType, true, new MetadataBuilder().build()),
				new StructField("title", DataTypes.StringType, true, new MetadataBuilder().build())
		});
		return sqlCtx.createDataFrame(vertices_rdd, vertices_schema);
	}

	private static Dataset<Row> loadEdges(JavaSparkContext ctx, SQLContext sqlCtx) throws IOException, URISyntaxException {
		List<Row> edges = new ArrayList<>();
		File file = new File(EDGES_RESOURCE.toURI());
		BufferedReader br = new BufferedReader(new FileReader(file));
		String st;
		while ((st = br.readLine()) != null) {
			String[] split = st.split("\t");
			long id1 = Long.parseLong(split[0]);
			long id2 = Long.parseLong(split[1]);
			edges.add(RowFactory.create(id1, id2, "link"));
		}

		JavaRDD<Row> edges_rdd = ctx.parallelize(edges);
		StructType edges_schema = new StructType(new StructField[]{
				new StructField("src", DataTypes.LongType, true, new MetadataBuilder().build()),
				new StructField("dst", DataTypes.LongType, true, new MetadataBuilder().build())
		});
		return sqlCtx.createDataFrame(edges_rdd, edges_schema);
	}

}