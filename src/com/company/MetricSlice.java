package com.company;

import java.util.Objects;
import java.util.concurrent.TimeUnit;


public final class MetricSlice {

  final long metricId;
  final long start;
  final long end;

  MetricSlice(long metricId, long start, long end) {
    this.metricId = metricId;
    this.start = start;
    this.end = end;
  }

  public long getMetricId() {
    return metricId;
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  public MetricSlice withStart(long start) {
    return new MetricSlice(metricId, start, end);
  }

  public MetricSlice withEnd(long end) {
    return new MetricSlice(metricId, start, end);
  }

  public static MetricSlice from(long metricId, long start, long end) {
    return new MetricSlice(metricId, start, end);
  }

  public boolean containSlice(MetricSlice slice) {
    return slice.metricId == this.metricId &&
        slice.start >= this.start && slice.end <= this.end;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MetricSlice that = (MetricSlice) o;
    return metricId == that.metricId && start == that.start && end == that.end;
  }

  @Override
  public int hashCode() {
    return Objects.hash(metricId, start, end);
  }
}
