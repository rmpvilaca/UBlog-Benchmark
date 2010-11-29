/*******************************************************************************
 * Copyright 2010 Universidade do Minho, Ricardo Vilaça and Francisco Cruz
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.ublog.utils;

public class Time {

	private final double valueInBaseUnit;

	private double valueInBaseUnit() {
		return valueInBaseUnit;
	}

	private double getValue(TimeUnit unit) {
		return valueInBaseUnit / unit.getRelationToBaseUnit();
	}

	private Time(double value, TimeUnit unit) {
		this.valueInBaseUnit = value * unit.getRelationToBaseUnit();
	}

	private Time(double valueInBaseUnit) {
		this.valueInBaseUnit = valueInBaseUnit;
	}

	public Time add(Time time) {
		return new Time(this.valueInBaseUnit + time.valueInBaseUnit());
	}

	public Time subtract(Time time) {
		return new Time(valueInBaseUnit() - time.valueInBaseUnit());
	}

	public Time multiply(double factor) {
		return new Time(valueInBaseUnit() * factor);
	}

	public Time divideBy(double divisor) {
		return multiply(1 / divisor);
	}

	public static final Time ZERO = new Time(0);
	public static final Time MAX_VALUE = new Time(Double.MAX_VALUE);
	public static final Time MIN_VALUE = new Time(Double.MIN_VALUE);

	/*
	 * All the different time units ...
	 */

	private static abstract class TimeUnit {
		public final double relationToBaseUnit;

		public double getRelationToBaseUnit() {
			return relationToBaseUnit;
		}

		private TimeUnit(double numberOfMilliseconds) {
			this.relationToBaseUnit = numberOfMilliseconds;
		}
	}

	private static Milliseconds MILLISECOND = new Milliseconds();

	private static class Milliseconds extends TimeUnit {
		private Milliseconds() {
			super(1);
		}
	}

	private static Seconds SECOND = new Seconds();

	private static class Seconds extends TimeUnit {
		private Seconds() {
			super(1e3);
		}
	}

	private static Minutes MINUTE = new Minutes();

	private static class Minutes extends TimeUnit {
		private Minutes() {
			super(60e3);
		}
	}

	private static Hours HOUR = new Hours();

	private static class Hours extends TimeUnit {
		private Hours() {
			super(3600e3);
		}
	}

	private static Days DAY = new Days();

	private static class Days extends TimeUnit {
		private Days() {
			super(86400e3);
		}
	}

	public static Time inMilliseconds(double value) {
		return new Time(value, Time.MILLISECOND);
	}

	/**
	 * Returns the <code>time</code> in milliseconds.
	 * 
	 * @return the <code>time</code> in milliseconds
	 */
	public static double inMilliseconds(Time time) {
		return time.getValue(Time.MILLISECOND);
	}

	/**
	 * Returns a <code>Time</code> object representing <code>value</code>
	 * seconds.
	 * 
	 * @param value
	 *            the time in seconds.
	 * @return a <code>Time</code> object representing <code>value</code>
	 *         seconds
	 */
	public static Time inSeconds(double value) {
		return new Time(value, Time.SECOND);
	}

	/**
	 * Returns the <code>time</code> in seconds.
	 * 
	 * @return the <code>time</code> in seconds
	 */
	public static double inSeconds(Time time) {
		return time.getValue(Time.SECOND);
	}

	/**
	 * Returns a <code>Time</code> object representing <code>value</code>
	 * minutes.
	 * 
	 * @param value
	 *            the time in minutes.
	 * @return a <code>Time</code> object representing <code>value</code>
	 *         minutes
	 */
	public static Time inMinutes(double value) {
		return new Time(value, Time.MINUTE);
	}

	public static double inMinutes(Time time) {
		return time.getValue(Time.MINUTE);
	}

	/**
	 * Returns a <code>Time</code> object representing <code>value</code> hours.
	 * 
	 * @param value
	 *            the time in hours.
	 * @return a <code>Time</code> object representing <code>value</code> hours
	 */
	public static Time inHours(double value) {
		return new Time(value, Time.HOUR);
	}

	/**
	 * Returns the <code>time</code> in hours.
	 * 
	 * @return the <code>time</code> in hours
	 */
	public static double inHours(Time time) {
		return time.getValue(Time.HOUR);
	}

	/**
	 * Returns a <code>Time</code> object representing <code>value</code> days.
	 * 
	 * @param value
	 *            the time in days.
	 * @return a <code>Time</code> object representing <code>value</code> days
	 */
	public static Time inDays(double value) {
		return new Time(value, Time.DAY);
	}

	/**
	 * Returns the <code>time</code> in days.
	 * 
	 * @return the <code>time</code> in days
	 */
	public static double inDays(Time time) {
		return time.getValue(Time.DAY);
	}

}
