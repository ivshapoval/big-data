package com.ververica.flinktraining.exercises.datastream_java.windows;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import com.ververica.flinktraining.exercises.datastream_java.utils.MissingSolutionException;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
public class HourlyTipsExercise extends ExerciseBase {

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get("input", ExerciseBase.pathToFareData);

		final int maxEventDelay = 60;
		final int servingSpeedFactor = 600;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		DataStream<TaxiFare> fareStream = env.addSource(
				fareSourceOrTest(new TaxiFareSource(input, maxEventDelay, servingSpeedFactor)));

		DataStream<Tuple3<Long, Long, Float>> hourlyMaxTips = fareStream
				.keyBy(fare -> fare.driverId)                       // группировка по идентификатору водителя
				.timeWindow(Time.hours(1))                          // оконное агрегирование по часу
				.process(new ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow>() {
					@Override
					public void process(Long driverId, Context context, Iterable<TaxiFare> fares,
										Collector<Tuple3<Long, Long, Float>> out) throws Exception {
						float totalTips = 0F;
						for (TaxiFare fare : fares) {
							totalTips += fare.tip;
						}
						out.collect(new Tuple3<>(context.window().getEnd(), driverId, totalTips));
					}
				})
				.timeWindowAll(Time.hours(1))
				.maxBy(2);

		printOrTest(hourlyMaxTips);
		env.execute("Hourly Tips (java)");
	}
}