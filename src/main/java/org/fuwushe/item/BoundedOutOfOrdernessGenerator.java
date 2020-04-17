package org.fuwushe.item;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public  class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<UserBehaviorVO> {

		private final long maxOutOfOrderness = 4000; // 4 seconds

		private long currentMaxTimestamp;

		@Override
		public long extractTimestamp(UserBehaviorVO element, long previousElementTimestamp) {
			long timestamp = element.getTimestamp();
			currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
			return timestamp;
		}

		@Override
		public Watermark getCurrentWatermark() {
			// return the watermark as current highest timestamp minus the out-of-orderness bound
			return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
		}
	}