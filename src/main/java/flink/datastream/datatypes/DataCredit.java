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

package flink.datastream.datatypes;

import com.dataartisans.flinktraining.exercises.datastream_java.utils.dataPrepare;
import org.joda.time.DateTime;

import java.io.Serializable;

public class DataCredit implements  Serializable {
	public Double Time;
	public Double TimeScaler;
	public Double V1;
	public Double V2;
	public Double V3;
	public Double V4;
	public Double V5;
	public Double V6;
	public Double V7;
	public Double V8;
	public Double V9;
	public Double V10;
	public Double V11;
	public Double V12;
	public Double V13;
	public Double V14;
	public Double V15;
	public Double V16;
	public Double V17;
	public Double V18;
	public Double V19;
	public Double V20;
	public Double V21;
	public Double V22;
	public Double V23;
	public Double V24;
	public Double V25;
	public Double V26;
	public Double V27;
	public Double V28;
	public Double Amount;
	public Double AmountScaler;
	public Double Class;
	public DateTime eventTime;

	public DataCredit() {
		this.eventTime = new DateTime();
	}

	public void TimeScalerParse(){
		this.TimeScaler= dataPrepare.Normalization(this.Time,172792.0, Double.valueOf(0));
	}

	public void AmountParse(){
		this.AmountScaler= dataPrepare.Normalization(this.Amount,25691.16, Double.valueOf(0));
	}


	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(TimeScaler).append(",");
		sb.append(V1).append(",");
		sb.append(V2).append(",");
		sb.append(V3).append(",");
		sb.append(V4).append(",");
		sb.append(V5).append(",");
		sb.append(V6).append(",");
		sb.append(V7).append(",");
		sb.append(V8).append(",");
		sb.append(V9).append(",");
		sb.append(V10).append(",");
		sb.append(V11).append(",");
		sb.append(V12).append(",");
		sb.append(V13).append(",");
		sb.append(V14).append(",");
		sb.append(V15).append(",");
		sb.append(V16).append(",");
		sb.append(V17).append(",");
		sb.append(V18).append(",");
		sb.append(V19).append(",");
		sb.append(V20).append(",");
		sb.append(V21).append(",");
		sb.append(V22).append(",");
		sb.append(V23).append(",");
		sb.append(V24).append(",");
		sb.append(V25).append(",");
		sb.append(V26).append(",");
		sb.append(V27).append(",");
		sb.append(V28).append(",");
		sb.append(AmountScaler).append(",");
		sb.append(Class);

		return sb.toString();
	}

	public static DataCredit instanceFromString(String line) {

		String[] tokens = line.split(",");
		if (tokens.length != 31) {
			System.out.println("#############Invalid record: " + line+"\n");
			//return null;
			//throw new RuntimeException("Invalid record: " + line);
		}

		DataCredit diag = new DataCredit();

		try {
			diag.Time = tokens[0].length() > 0 ? Double.parseDouble(tokens[0].trim()):null;
			diag.V1 = tokens[1].length() > 0 ? Double.parseDouble(tokens[1]):null;
			diag.V2 = tokens[2].length() > 0 ? Double.parseDouble(tokens[2]): null;
			diag.V3 = tokens[3].length() > 0 ? Double.parseDouble(tokens[3]) : null;
			diag.V4 = tokens[4].length() > 0 ? Double.parseDouble(tokens[4]) : null;
			diag.V5 = tokens[5].length() > 0 ? Double.parseDouble(tokens[5]): null;
			diag.V6 =tokens[6].length() > 0 ? Double.parseDouble(tokens[6]) : null;
			diag.V7 = tokens[7].length() > 0 ? Double.parseDouble(tokens[7]) : null;
			diag.V8 = tokens[8].length() > 0 ? Double.parseDouble(tokens[8]) : null;
			diag.V9 = tokens[9].length() > 0 ? Double.parseDouble(tokens[9]) : null;
			diag.V10 = tokens[10].length() > 0 ? Double.parseDouble(tokens[10]) : null;
			diag.V11 = tokens[11].length() > 0 ? Double.parseDouble(tokens[11]) : null;
			diag.V12 = tokens[12].length() > 0 ? Double.parseDouble(tokens[12]) : null;
			diag.V13 = tokens[13].length() > 0 ? Double.parseDouble(tokens[13]) : null;
			diag.V14 = tokens[14].length() > 0 ? Double.parseDouble(tokens[14]) : null;
			diag.V15 = tokens[15].length() > 0 ? Double.parseDouble(tokens[15]) : null;
			diag.V16 = tokens[16].length() > 0 ? Double.parseDouble(tokens[16]) : null;
			diag.V17 = tokens[17].length() > 0 ? Double.parseDouble(tokens[17]) : null;
			diag.V18 = tokens[18].length() > 0 ? Double.parseDouble(tokens[18]) : null;
			diag.V19 = tokens[19].length() > 0 ? Double.parseDouble(tokens[19]) : null;
			diag.V20 = tokens[20].length() > 0 ? Double.parseDouble(tokens[20]) : null;
			diag.V21 = tokens[21].length() > 0 ? Double.parseDouble(tokens[21]) : null;
			diag.V22 = tokens[22].length() > 0 ? Double.parseDouble(tokens[22]) : null;
			diag.V23 = tokens[23].length() > 0 ? Double.parseDouble(tokens[23]) : null;
			diag.V24 = tokens[24].length() > 0 ? Double.parseDouble(tokens[24]) : null;
			diag.V25 = tokens[25].length() > 0 ? Double.parseDouble(tokens[25]) : null;
			diag.V26 = tokens[26].length() > 0 ? Double.parseDouble(tokens[26]) : null;
			diag.V27 = tokens[27].length() > 0 ? Double.parseDouble(tokens[27]) : null;
			diag.V28 = tokens[28].length() > 0 ? Double.parseDouble(tokens[28]) : null;
			diag.Amount = tokens[29].length() > 0 ? Double.parseDouble(tokens[29]) : null;
			diag.Class = tokens[30].length() > 0 ? Double.parseDouble(tokens[30]) : null;
		} catch (NumberFormatException nfe) {
			throw new RuntimeException("Invalid record: " + line, nfe);
		}
		return diag;
	}

	public long getEventTime() {
		return this.eventTime.getMillis();
	}
}
