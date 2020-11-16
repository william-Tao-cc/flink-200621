package com.atguigu.day01.Flink01_WordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author tccstart
 * @create 2020-11-16 19:06
 */
public class WordCount_Bounded {

    public static void main(String[] args) throws Exception {

        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取文本数据
        DataStreamSource<String> lineDS = env.readTextFile("input");

        //压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> WordToOneDS = lineDS.flatMap(new myFlat());

        //聚合
        KeyedStream<Tuple2<String, Integer>, Tuple> keyDS = WordToOneDS.keyBy(0);

        //计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyDS.sum(1);

        //打印
        result.print();
    }

    private static class myFlat implements FlatMapFunction<String,Tuple2<String,Integer>> {
        @Override
        //value 是输入值类型, 是我们一行一行的word
        //out 是输出值
        public void flatMap(String value, Collector<Tuple2<String,Integer>> out) throws Exception {

            //首先对输入值切开，进来的数据是一行一行的，所以切开以后存在数组中
            String[] words = value.split(" ");

            //遍历每一各单词，添加到输出值中去
            for (String word : words) {
                out.collect(new Tuple2<>(word,1));
            }
        }
    }
}
