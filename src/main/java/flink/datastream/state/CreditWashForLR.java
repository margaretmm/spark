/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package flink.datastream.state;


import flink.datastream.datatypes.DataCredit;
import flink.datastream.sources.CreditSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CreditWashForLR {

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = "D:/code/dataML/creditcardfraud/creditcard.csv";
		final int servingSpeedFactor = 6000; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// operate in Event-time
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// start the data generator
		DataStream<DataCredit> DsHeart = env.addSource(
				new CreditSource(input, servingSpeedFactor));

		DataStream<DataCredit> modDataStr = DsHeart
				.filter(new NoneFilter())
				.map(new mapTime()).keyBy(0)
				.flatMap(new NullFillMean());//使用T1 预测T3 因为相关性大

		DataStream<String> modFraudStrForLR =modDataStr.filter(new FraudFilter()).flatMap(new CreditFlatMapForLR());
		DataStream<String> modNonFraudStrForLR =modDataStr.filter(new NonFraudFilter()).flatMap(new CreditFlatMapForLR());
		//modDataStr2.print();
		modFraudStrForLR.writeAsText("./CreditDataFraudForLR");
		modNonFraudStrForLR.writeAsText("./CreditDataNonFraudForLR");
		// run the prediction pipeline
		env.execute("HeartData Prediction");
	}


	public static class mapTime implements MapFunction<DataCredit, Tuple2<Long, DataCredit>> {
		@Override
		public Tuple2<Long, DataCredit> map(DataCredit energy) throws Exception {
			long time = energy.eventTime.getMillis();;

			return new Tuple2<>(time, energy);
		}
	}

	public static class FraudFilter implements FilterFunction<DataCredit> {
		@Override
		public boolean filter(DataCredit credit) throws Exception {
			return credit.Class==1;
		}
	}

	public static class NonFraudFilter implements FilterFunction<DataCredit> {
		@Override
		public boolean filter(DataCredit credit) throws Exception {
			return credit.Class==0;
		}
	}

	public static class NoneFilter implements FilterFunction<DataCredit> {
		@Override
		public boolean filter(DataCredit credit) throws Exception {
			return IsNotNone(credit.Time) && IsNotNone(credit.V1) &&IsNotNone(credit.V2)
					 && IsNotNone(credit.Class) ;
		}

		public boolean IsNotNone(Object Data){
			if (Data == null )
				return false;
			else
				return true;
		}
	}

    //使用Spark ML RF, 数据格式保存为csv常见格式
	// label 转换成一个二元变量表示
	public static class CreditFlatMapForLR implements FlatMapFunction<DataCredit, String> {

		@Override
		public void flatMap(DataCredit InputDiag, Collector<String> collector) throws Exception {
			DataCredit sale = InputDiag;

     		//sb.append(diag.date).append(",");
			sale.TimeScalerParse();
			sale.AmountParse();

			collector.collect(sale.toString());
		}
	}

	public static class NullFillMean extends RichFlatMapFunction<Tuple2<Long, DataCredit> ,DataCredit> {
		//private transient ValueState<Double> heartMeanState;
		private transient ListState<Double> ProductCategoryState;
		private List<Double> meansList;

		@Override
		public void flatMap(Tuple2<Long, DataCredit>  val, Collector< DataCredit> out) throws Exception {
			Iterator<Double> modStateLst = ProductCategoryState.get().iterator();
			Double MeanProductCategory1=null;
			Double MeanProductCategory2=null;

			if(!modStateLst.hasNext()){
				MeanProductCategory1 = 0.55;
				MeanProductCategory2 = 1.0;
			}else{
				MeanProductCategory1=modStateLst.next();
				MeanProductCategory2=modStateLst.next();
			}

			DataCredit credit = val.f1;

			if(credit.V16 == null){
				credit.V16= MeanProductCategory1;
			}else if(credit.V12 == null){
				credit.V12= MeanProductCategory2;
			}else
			{
                meansList= new ArrayList<Double>();
                meansList.add(credit.V16);
                meansList.add(credit.V12);
				ProductCategoryState.update(meansList);
			}
			out.collect(credit);
		}

		@Override
		public void open(Configuration config) {
			ListStateDescriptor<Double> descriptor2 =
					new ListStateDescriptor<>(
							// state name
							"regressionModel",
							// type information of state
							TypeInformation.of(Double.class));
			ProductCategoryState = getRuntimeContext().getListState(descriptor2);
		}
	}
}
