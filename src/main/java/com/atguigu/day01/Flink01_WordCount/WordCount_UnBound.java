package com.atguigu.day01.Flink01_WordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * @author tccstart
 * @create 2020-11-16 15:03
 */
public class WordCount_UnBound {

    public static void main(String[] args) throws Exception {

        //创建flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取数据流
        DataStreamSource<String> lineDS = env.socketTextStream("hadoop102", 7777);

        //对数据压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordOneDS = lineDS.flatMap(new myFlat2());

        //用keyBy聚合
        KeyedStream<Tuple2<String, Integer>, Tuple> keyByDS = wordOneDS.keyBy(0);

        //聚合后累加
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumDS = keyByDS.sum(1);

        //打印
        sumDS.print();

        //执行无界流，相当于要阻塞线程
        env.execute("WordCount_UnBound");

    }
    //FlatMapFunction 泛型写我们的输入类型 word --> String 和输出类型 （word，1）--> Tuple2<String,Integer>
    private static class myFlat2 implements FlatMapFunction<String,Tuple2<String,Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {

            //输入的是一行行数据 line, 切开以后保存的数组中
            String[] words = line.split(" ");

            //遍历单词, 返回，out就是我们返回值，所以往out中添加值
            for (String word : words) {
                out.collect(new Tuple2<>(word,1));
            }

        }
    }
}
