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

import org.joda.time.DateTime;

import java.io.Serializable;

/**
 * A TaxiRide is a taxi ride event. There are two types of events, a taxi ride start event and a
 * taxi ride end event. The isStart flag specifies the type of the event.
 *
 * A TaxiRide consists of
 * - the rideId of the event which is identical for start and end record
 * - the type of the event (start or end)
 * - the time of the event
 * - the longitude of the start location
 * - the latitude of the start location
 * - the longitude of the end location
 * - the latitude of the end location
 * - the passengerCnt of the ride
 * - the taxiId
 * - the driverId
 *
 */
public class DataTaxiFare implements  Serializable {
	public String vendor_id;
	public int rate_code;
	public int passenger_count;
	public int trip_time_in_secs;
	public Double trip_distance;
	public String payment_type	;
	public Double fare_amount;
	public DateTime eventTime;

	public DataTaxiFare() {
		this.eventTime = new DateTime();
	}

	public DataTaxiFare(String vendor_id, int rate_code, int passenger_count, int trip_time_in_secs,
						Double trip_distance, String payment_type,Double fare_amount) {
		this.eventTime = new DateTime();
		this.vendor_id = vendor_id;
		this.rate_code = rate_code;
		this.passenger_count = passenger_count;
		this.trip_time_in_secs = trip_time_in_secs;
		this.trip_distance = trip_distance;
		this.payment_type = payment_type;
		this.fare_amount = fare_amount;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(vendor_id).append(",");
		sb.append(rate_code).append(",");
		sb.append(passenger_count).append(",");
		sb.append(trip_time_in_secs).append(",");
		sb.append(trip_distance).append(",");
		sb.append(payment_type).append(",");
		sb.append(fare_amount);

		return sb.toString();
	}

	public static DataTaxiFare instanceFromString(String line) {

		String[] tokens = line.split(",");
		if (tokens.length != 7) {
			System.out.println("#############Invalid record: " + line+"\n");
			//return null;
			//throw new RuntimeException("Invalid record: " + line);
		}

		DataTaxiFare diag = new DataTaxiFare();

		try {
			diag.vendor_id = tokens[0].length() > 0 ? tokens[0]:null;
			diag.rate_code = tokens[1].length() > 0 ? Integer.parseInt(tokens[1]):null;
			diag.passenger_count = tokens[2].length() > 0 ? Integer.parseInt(tokens[2]): null;
			diag.trip_time_in_secs = tokens[3].length() > 0 ? Integer.parseInt(tokens[3]) : null;
			diag.trip_distance = tokens[4].length() > 0 ? Double.parseDouble(tokens[4]): null;
			diag.payment_type = tokens[5].length() > 0 ? tokens[5] : null;
			diag.fare_amount =tokens[6].length() > 0 ? Double.parseDouble(tokens[6]) : null;

		} catch (NumberFormatException nfe) {
			throw new RuntimeException("Invalid record: " + line, nfe);
		}
		return diag;
	}

	public long getEventTime() {
		return this.eventTime.getMillis();
	}

}
