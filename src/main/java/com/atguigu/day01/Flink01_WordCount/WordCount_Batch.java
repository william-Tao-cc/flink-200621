package com.atguigu.day01.Flink01_WordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author tccstart
 * @create 2020-11-16 14:42
 * <p>
 * Flink 批处理
 */
public class WordCount_Batch {

    public static void main(String[] args) throws Exception {

        //1.创建flink程序的入口
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.读取文件数据 "hello atguigu"
        DataSource<String> lineDS = env.readTextFile("input");

        //3.对每一行数据压平操作了
        FlatMapOperator<String, Tuple2<String, Integer>> wordToOneDS = lineDS.flatMap(new MyFlatMapFunc());

        //4.分组
        UnsortedGrouping<Tuple2<String, Integer>> groupByDS = wordToOneDS.groupBy(0);

        //5.聚合计算
        AggregateOperator<Tuple2<String, Integer>> result = groupByDS.sum(1);

        //6.打印结果
        result.print();

        /**
         * Flink对于每一条数据都是实时处理了，groupByKey是批处理，有一批数据然后groupByKey
         * 流处理，是针对每一条数据，首先进来一条数据，我们得先确定其分组，然后再sum累加
         * spark中是批处理，因此可以groupByKey
         */


    }

    public static class MyFlatMapFunc implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String vaule, Collector<Tuple2<String, Integer>> out) throws Exception {

            //按照空格切分value
            String[] words = vaule.split(" ");

            //遍历word输出数据
            for (String word : words) {
                out.collect(new Tuple2<>(word,1));
            }
        }
    }
}