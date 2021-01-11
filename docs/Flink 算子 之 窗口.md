## 窗口

窗口是处理无限流的核心。窗口分割无限流形成有限大小的的桶，我们可以通过桶来进行计算。Fink窗口计算通用结构可以分成两种：keyed streams 和 non-keyed streams。

### Keyed Windows

```
stream
       .keyBy(...)               <-  keyed versus non-keyed windows
       .window(...)              <-  必选: "assigner"
      [.trigger(...)]            <-  可选: "trigger" (else default trigger)
      [.evictor(...)]            <-  可选: "evictor" (else no evictor)
      [.allowedLateness(...)]    <-  可选: "lateness" (else zero)
      [.sideOutputLateData(...)] <-  可选: "output tag" (else no side output for late data)
       .reduce/aggregate/fold/apply()      <-  必选: "function"
      [.getSideOutput(...)]      <-  可选: "output tag"
```

### Non-Keyed Windows

```
stream
       .windowAll(...)           <-  必选: "assigner"
      [.trigger(...)]            <-  可选: "trigger" (else default trigger)
      [.evictor(...)]            <-  可选: "evictor" (else no evictor)
      [.allowedLateness(...)]    <-  可选: "lateness" (else zero)
      [.sideOutputLateData(...)] <-  可选: "output tag" (else no side output for late data)
       .reduce/aggregate/fold/apply()      <-  必选: "function"
      [.getSideOutput(...)]      <-  可选: "output tag"
```

## 窗口生命周期

窗口的生命周期指的是一个被创建的窗口从第一个元素进入窗口到处理时间结束。

## Keyed vs Non-Keyed 窗口

在定义窗口之前,要指定的第一件事是流是否需要Keyed，使用keyBy（…）将无界流分成逻辑的keyed stream。 如果未调用keyBy（…），则表示流不是keyed stream。

对于Keyed流:可以将传入事件的任何属性用作key。 拥有Keyed stream将允许窗口计算由多个任务并行执行，因为每个逻辑Keyed流可以独立于其余任务进行处理。 相同Key的所有元素将被发送到同一个任务。
对于Non-Keyed流：原始流将不会被分成多个逻辑流，并且所有窗口逻辑将由单个Task执行，即并行性为1。

## 窗口分配器

Flink预定义了通用的窗口，滚动窗口、滑动窗口、会话窗口和全局窗口。

### 滚动窗口

滚动窗口分配器分配每个元素到制定大小的窗口。滚动窗口有固定的大小并且不会重叠。比如，你指定了5分钟的滚动窗口，每隔5分钟当前的窗口将计算，新的窗口将创建。
```
DataStream<T> input = ...;

// tumbling event-time windows
input
    .keyBy(<key selector>)
    .window(TumblingEventTimeWindows.of(Time.seconds(5)))
    .<windowed transformation>(<window function>);

// tumbling processing-time windows
input
    .keyBy(<key selector>)
    .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
    .<windowed transformation>(<window function>);

// daily tumbling event-time windows offset by -8 hours.
input
    .keyBy(<key selector>)
    .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
    .<windowed transformation>(<window function>);
```

### 滑动窗口
滑动窗口分配器分配元素到固定长度的窗口。和滚动窗口类似，窗口的大小可以通过窗口大小参数指定，额外的可以通过滑动参数指定滑动的频率。
因此滑动大小小于窗口大小窗口就会重叠。这种情况下，元素会分配给多个窗口。
```
DataStream<T> input = ...;

// sliding event-time windows
input
    .keyBy(<key selector>)
    .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
    .<windowed transformation>(<window function>);

// sliding processing-time windows
input
    .keyBy(<key selector>)
    .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
    .<windowed transformation>(<window function>);

// sliding processing-time windows offset by -8 hours
input
    .keyBy(<key selector>)
    .window(SlidingProcessingTimeWindows.of(Time.hours(12), Time.hours(1), Time.hours(-8)))
    .<windowed transformation>(<window function>);
```

### 会话窗口

## 参考
1. https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/stream/operators/windows.html
2. https://blog.csdn.net/vincent_duan/article/details/102619887