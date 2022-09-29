package exercise_3;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Exercise_3 {

    private static final Map<Long, String> ID_TO_LABEL = ImmutableMap.<Long, String>builder()
            .put(1L, "A")
            .put(2L, "B")
            .put(3L, "C")
            .put(4L, "D")
            .put(5L, "E")
            .put(6L, "F")
            .build();

    private static class VProg
            extends AbstractFunction3<Long,Tuple2<Integer, String>,Tuple2<Integer, String>,Tuple2<Integer, String>>
            implements Serializable {
        @Override
        public Tuple2<Integer, String> apply(Long vertexID, Tuple2<Integer, String> vertexTuple, Tuple2<Integer, String> message) {
            if (vertexTuple._1() != Integer.MAX_VALUE && vertexTuple._1() < message._1())
                return vertexTuple;
            return message;
        }
    }

    private static class sendMsg
            extends AbstractFunction1<EdgeTriplet<Tuple2<Integer, String>,Integer>, Iterator<Tuple2<Object, Tuple2<Integer, String>>>>
            implements Serializable {
        @Override
        public Iterator<Tuple2<Object, Tuple2<Integer, String>>> apply(EdgeTriplet<Tuple2<Integer, String>, Integer> triplet) {
            Integer srcWeight = triplet.srcAttr()._1();
            String srcPath = triplet.srcAttr()._2();
            Integer dstWeight = triplet.dstAttr()._1();
            Integer edgeWeight = triplet.toTuple()._3();
            Integer weight = srcWeight + edgeWeight;
            String path = srcPath + "," + ID_TO_LABEL.get(triplet.dstId());
            if (srcWeight == Integer.MAX_VALUE || srcWeight + edgeWeight >= dstWeight)
                return JavaConverters.asScalaIteratorConverter(
                        new ArrayList<Tuple2<Object, Tuple2<Integer, String>>>().iterator()
                ).asScala();
            return JavaConverters.asScalaIteratorConverter(
                    Arrays.asList(new Tuple2<Object, Tuple2<Integer, String>>(triplet.dstId(), new Tuple2<>(weight, path)))
                            .iterator()
            ).asScala();
        }
    }

    private static class merge
            extends AbstractFunction2<Tuple2<Integer, String>,Tuple2<Integer, String>,Tuple2<Integer, String>>
            implements Serializable {
        @Override
        public Tuple2<Integer, String> apply(Tuple2<Integer, String> t1, Tuple2<Integer, String> t2) {
            if (t1._1() < t2._1())
                return t1;
            return t2;
        }
    }

    public static void shortestPathsExt(JavaSparkContext ctx) {
        List<Tuple2<Object, Tuple2<Integer, String>>> vertices = Lists.newArrayList(
                new Tuple2<>(1L, new Tuple2<>(0, "A")),
                new Tuple2<>(2L, new Tuple2<>(Integer.MAX_VALUE, "")),
                new Tuple2<>(3L, new Tuple2<>(Integer.MAX_VALUE, "")),
                new Tuple2<>(4L, new Tuple2<>(Integer.MAX_VALUE, "")),
                new Tuple2<>(5L, new Tuple2<>(Integer.MAX_VALUE, "")),
                new Tuple2<>(6L, new Tuple2<>(Integer.MAX_VALUE, ""))
        );

        List<Edge<Integer>> edges = Lists.newArrayList(
                new Edge<>(1L,2L, 4), // A --> B (4)
                new Edge<>(1L,3L, 2), // A --> C (2)
                new Edge<>(2L,3L, 5), // B --> C (5)
                new Edge<>(2L,4L, 10), // B --> D (10)
                new Edge<>(3L,5L, 3), // C --> E (3)
                new Edge<>(5L, 4L, 4), // E --> D (4)
                new Edge<>(4L, 6L, 11) // D --> F (11)
        );

        JavaRDD<Tuple2<Object, Tuple2<Integer, String>>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);
        Graph<Tuple2<Integer, String>, Integer> G = Graph.apply(verticesRDD.rdd(), edgesRDD.rdd(),new Tuple2<>(0, ""), StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class)); //TODO third argument

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));
        ops.pregel(new Tuple2<>(Integer.MAX_VALUE, ""),
                        Integer.MAX_VALUE,
                        EdgeDirection.Out(),
                        new Exercise_3.VProg(),
                        new Exercise_3.sendMsg(),
                        new Exercise_3.merge(),
                        ClassTag$.MODULE$.apply(Tuple2.class))
                .vertices()
                .toJavaRDD()
                .sortBy(f -> ((Tuple2<Object, Tuple2<Integer, String>>) f)._1, true, 0)
                .foreach(v -> {
                    Tuple2<Object,Tuple2<Integer, String>> vertex = (Tuple2<Object,Tuple2<Integer, String>>)v;
                    System.out.println("Minimum path to get from "+ ID_TO_LABEL.get(1L)+" to "+ID_TO_LABEL.get(vertex._1)+" is [" + vertex._2._2 + "] with cost " + vertex._2._1);
                });
    }

}
