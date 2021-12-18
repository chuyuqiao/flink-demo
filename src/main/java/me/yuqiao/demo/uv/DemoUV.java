package me.yuqiao.demo.uv;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public class DemoUV {
    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);
        String brokers = params.get("brokers");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source =
                KafkaSource.<String>builder()
                        .setBootstrapServers("127.0.0.1:9092")
                        .setTopics("input-topic")
                        .setGroupId("my-group")
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build();

        KafkaSink sink =
                KafkaSink.<OutputPvUvResult>builder()
                        .setBootstrapServers("127.0.0.1:9092")
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic("topic-name")
                                        .setValueSerializationSchema(
                                                new OutputPvUvResultSerialization())
                                        .build())
                        .build();

        DataStream<String> sourceStream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        DataStream<UserActionRecord> watermarkedStream =
                sourceStream
                        .map(
                                (message) -> {
                                    UserActionRecord record =
                                            JSON.parseObject(message, UserActionRecord.class);
                                    String platform = record.getPlatform();
                                    record.setPlatform(
                                            platform
                                                    + "@"
                                                    + ThreadLocalRandom.current().nextInt(20));
                                    return record;
                                })
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(30)));

        WindowedStream<UserActionRecord, String, TimeWindow> windowedStream =
                watermarkedStream
                        .keyBy(UserActionRecord::getPlatform)
                        .window(TumblingEventTimeWindows.of(Time.minutes(1)));

        DataStream<WindowedViewSum> minutelyPartialAggStream =
                windowedStream.aggregate(new ViewAggregateFunc(), new ViewSumWindowFunc());

        minutelyPartialAggStream
                .keyBy(value -> value.windowEndTimestamp)
                .process(new OutputPvUvProcessFunc(), TypeInformation.of(OutputPvUvResult.class))
                .sinkTo(sink)
                .name("Kafka Sink")
                .setParallelism(1);

        System.out.println(env.getStreamGraph().getStreamingPlanAsJSON());
    }

    private static class ViewAggregateFunc
            implements AggregateFunction<UserActionRecord, ViewAccumulator, ViewAccumulator> {

        @Override
        public ViewAccumulator createAccumulator() {
            return new ViewAccumulator();
        }

        @Override
        public ViewAccumulator add(UserActionRecord value, ViewAccumulator accumulator) {
            if (accumulator.getKey().isEmpty()) {
                accumulator.setKey(value.getPlatform());
            }
            accumulator.addCount(1);
            accumulator.addUserId(value.getUserId());
            return accumulator;
        }

        @Override
        public ViewAccumulator getResult(ViewAccumulator accumulator) {
            return accumulator;
        }

        @Override
        public ViewAccumulator merge(ViewAccumulator a, ViewAccumulator b) {
            if (a.getKey().isEmpty()) {
                a.setKey(b.getKey());
            }
            a.addCount(b.getCount());
            a.addUserIds(b.getUserIds());
            return a;
        }
    }

    private static class ViewAccumulator extends Tuple3<String, Integer, Set<String>> {
        public ViewAccumulator() {
            super("", 0, new HashSet<>(2048));
        }

        public ViewAccumulator(String key, int count, Set<String> userIds) {
            super(key, count, userIds);
        }

        public String getKey() {
            return this.f0;
        }

        public void setKey(String key) {
            this.f0 = key;
        }

        public int getCount() {
            return this.f1;
        }

        public void addCount(int count) {
            this.f1 += count;
        }

        public Set<String> getUserIds() {
            return this.f2;
        }

        public void addUserId(String userId) {
            this.f2.add(userId);
        }

        public void addUserIds(Set<String> userIds) {
            this.f2.addAll(userIds);
        }
    }

    private static class ViewSumWindowFunc
            implements WindowFunction<ViewAccumulator, WindowedViewSum, String, TimeWindow> {
        @Override
        public void apply(
                String tuple,
                TimeWindow window,
                Iterable<ViewAccumulator> input,
                Collector<WindowedViewSum> out)
                throws Exception {
            ViewAccumulator acc = input.iterator().next();
            String key = acc.getKey();
            out.collect(
                    new WindowedViewSum(
                            key.substring(0, key.indexOf("@")),
                            window.getStart(),
                            window.getEnd(),
                            acc.getCount(),
                            acc.getUserIds()));
        }
    }

    @Data
    @AllArgsConstructor
    @EqualsAndHashCode
    private static class WindowedViewSum implements Comparable<WindowedViewSum> {
        private String key;
        private Long windowStartTimestamp;
        private Long windowEndTimestamp;
        private int pv;
        private Set<String> userIds;

        @Override
        public int compareTo(WindowedViewSum o) {
            return (int) (this.windowEndTimestamp - o.windowEndTimestamp);
        }
    }

    private static class OutputPvUvProcessFunc
            extends KeyedProcessFunction<Long, WindowedViewSum, OutputPvUvResult> {
        private static final String TIME_MINUTE_FORMAT = "yyyy-MM-dd HH:mm";
        private ListState<WindowedViewSum> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            state =
                    this.getRuntimeContext()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "state_windowed_pvuv_sum", WindowedViewSum.class));
        }

        @Override
        public void processElement(
                WindowedViewSum value,
                KeyedProcessFunction<Long, WindowedViewSum, OutputPvUvResult>.Context ctx,
                Collector<OutputPvUvResult> out)
                throws Exception {
            state.add(value);
            ctx.timerService().registerEventTimeTimer(value.getWindowEndTimestamp() + 1);
        }

        @Override
        public void onTimer(
                long timestamp,
                KeyedProcessFunction<Long, WindowedViewSum, OutputPvUvResult>.OnTimerContext ctx,
                Collector<OutputPvUvResult> out)
                throws Exception {
            Map<String, Tuple2<Integer, Set<String>>> result = new HashMap<>();
            String timeInMinute = "";
            for (WindowedViewSum viewSum : state.get()) {
                if (timeInMinute.isEmpty()) {
                    LocalDateTime date = LocalDateTime.now();
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(TIME_MINUTE_FORMAT);
                    timeInMinute = date.format(formatter);
                }
                String key = viewSum.getKey();
                if (!result.containsKey(key)) {
                    result.put(key, new Tuple2<>(0, new HashSet<>(2048)));
                }
                Tuple2<Integer, Set<String>> puv = result.get(key);
                puv.f0 += viewSum.getPv();
                puv.f1.addAll(viewSum.getUserIds());
            }

            JSONObject json = new JSONObject();
            for (Map.Entry<String, Tuple2<Integer, Set<String>>> entry : result.entrySet()) {
                String key = entry.getKey();
                Tuple2<Integer, Set<String>> value = entry.getValue();
                json.put(key.concat("_pv"), value.f0);
                json.put(key.concat("_uv"), value.f1.size());
            }
            json.put("time", timeInMinute.substring(11));

            state.clear();
            out.collect(
                    new OutputPvUvResult(
                            timeInMinute.substring(0, 10),
                            timeInMinute.substring(11),
                            json.toJSONString()));
        }
    }

    @Data
    @AllArgsConstructor
    private static class OutputPvUvResult {
        private String date;
        private String time;
        private String pvUv;
    }

    private static class OutputPvUvResultSerialization<RowData>
            implements SerializationSchema<RowData> {

        @Override
        public byte[] serialize(RowData element) {
            return new byte[0];
        }
    }

    @Data
    private static class UserActionRecord {
        private String userId;
        private String platform;
        private String page;
        private Long timestamp;
    }
}
