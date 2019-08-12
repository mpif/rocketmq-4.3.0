/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.common.stats;

import java.util.LinkedList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.InternalLogger;

public class StatsItem {

    private final AtomicLong value = new AtomicLong(0);

    private final AtomicLong times = new AtomicLong(0);

    private final LinkedList<CallSnapshot> csListMinute = new LinkedList<CallSnapshot>();

    private final LinkedList<CallSnapshot> csListHour = new LinkedList<CallSnapshot>();

    private final LinkedList<CallSnapshot> csListDay = new LinkedList<CallSnapshot>();

    private final String statsName;
    private final String statsKey;
    private final ScheduledExecutorService scheduledExecutorService;
    private final InternalLogger log;

    public StatsItem(String statsName, String statsKey, ScheduledExecutorService scheduledExecutorService,
        InternalLogger log) {
        this.statsName = statsName;
        this.statsKey = statsKey;
        this.scheduledExecutorService = scheduledExecutorService;
        this.log = log;
    }

    private static StatsSnapshot computeStatsData(final LinkedList<CallSnapshot> csList) {
        StatsSnapshot statsSnapshot = new StatsSnapshot();
        synchronized (csList) {
            double tps = 0;
            double avgpt = 0;
            long sum = 0;
            if (!csList.isEmpty()) {
                CallSnapshot first = csList.getFirst();
                CallSnapshot last = csList.getLast();
                /**
                 * value这里指消息数量,两个value相减,则是统计单位时间内的消息总量
                 */
                sum = last.getValue() - first.getValue();
                /**
                 * tps,是Transactions Per Second的缩写，也就是事务数/秒.
                 * 而timestamp的单位是毫秒,所以这下面这句话等价于：
                 * tps = (sum * 1.0d) / ((last.getTimestamp() - first.getTimestamp()) / 1000);
                 *
                 * sum是单位时间内的总数
                 * (last.getTimestamp() - first.getTimestamp())计算的是时间总数(单位:秒)
                 * 总数/时间总数=事务数/秒
                 */
                tps = (sum * 1000.0d) / (last.getTimestamp() - first.getTimestamp());

                /**
                 * 统计次数
                 */
                long timesDiff = last.getTimes() - first.getTimes();
                if (timesDiff > 0) {
                    avgpt = (sum * 1.0d) / timesDiff;
                }
            }

            statsSnapshot.setSum(sum);
            statsSnapshot.setTps(tps);
            statsSnapshot.setAvgpt(avgpt);
        }

        return statsSnapshot;
    }

    public StatsSnapshot getStatsDataInMinute() {
        return computeStatsData(this.csListMinute);
    }

    public StatsSnapshot getStatsDataInHour() {
        return computeStatsData(this.csListHour);
    }

    public StatsSnapshot getStatsDataInDay() {
        return computeStatsData(this.csListDay);
    }

    public void init() {

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    /**
                     * 10秒钟采样1次
                     */
                    samplingInSeconds();
                } catch (Throwable ignored) {
                }
            }
        }, 0, 10, TimeUnit.SECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    /**
                     * 10分钟采样一次
                     */
                    samplingInMinutes();
                } catch (Throwable ignored) {
                }
            }
        }, 0, 10, TimeUnit.MINUTES);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    /**
                     * 1个小时采样一次
                     */
                    samplingInHour();
                } catch (Throwable ignored) {
                }
            }
        }, 0, 1, TimeUnit.HOURS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    printAtMinutes();
                } catch (Throwable ignored) {
                }
            }
        }, Math.abs(UtilAll.computNextMinutesTimeMillis() - System.currentTimeMillis()), 1000 * 60, TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    printAtHour();
                } catch (Throwable ignored) {
                }
            }
        }, Math.abs(UtilAll.computNextHourTimeMillis() - System.currentTimeMillis()), 1000 * 60 * 60, TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    printAtDay();
                } catch (Throwable ignored) {
                }
            }
        }, Math.abs(UtilAll.computNextMorningTimeMillis() - System.currentTimeMillis()) - 2000, 1000 * 60 * 60 * 24, TimeUnit.MILLISECONDS);
    }

    public void samplingInSeconds() {
        synchronized (this.csListMinute) {
            /**
             * 插入链表尾部
             */
            this.csListMinute.add(new CallSnapshot(System.currentTimeMillis(), this.times.get(), this.value
                .get()));
            /**
             * 因为是10秒中采样一次,所以一分钟内有6个10秒;
             * 又由于它是通过最后时间戳-开始时间戳的方式来统计时间的,所以需要多存一个。
             * 比如统计2019-08-12 12:00:01到2019-08-12 12:01:01时间段的tps，经过了1分钟，每10秒采样一次，则csListminute记录的值有如下7个：
             * 2019-08-12 12:00:01、
             * 2019-08-12 12:00:11、
             * 2019-08-12 12:00:21、
             * 2019-08-12 12:00:31、
             * 2019-08-12 12:00:41、
             * 2019-08-12 12:00:51、
             * 2019-08-12 12:01:01
             * 所以只经过了1分钟、60秒钟的时间，但是却要记录7个时间值.
             */
            if (this.csListMinute.size() > 7) {
                /**
                 * 删除链表头结点
                 */
                this.csListMinute.removeFirst();
            }
        }
    }

    public void samplingInMinutes() {
        synchronized (this.csListHour) {
            this.csListHour.add(new CallSnapshot(System.currentTimeMillis(), this.times.get(), this.value
                .get()));
            /**
             * 10分钟采样一次,一个小时有6个10分钟,由于是通过结束时间减去开始时间的方式统计时间,所以需要记录7次。
             */
            if (this.csListHour.size() > 7) {
                this.csListHour.removeFirst();
            }
        }
    }

    public void samplingInHour() {
        synchronized (this.csListDay) {
            this.csListDay.add(new CallSnapshot(System.currentTimeMillis(), this.times.get(), this.value
                .get()));
            /**
             * 一个小时采样一次, 1天24个小时,由于是通过结束时间减去开始时间的方式统计时间,所以需要记录25次。
             */
            if (this.csListDay.size() > 25) {
                this.csListDay.removeFirst();
            }
        }
    }

    public void printAtMinutes() {
        StatsSnapshot ss = computeStatsData(this.csListMinute);
        log.info(String.format("[%s] [%s] Stats In One Minute, SUM: %d TPS: %.2f AVGPT: %.2f",
            this.statsName,
            this.statsKey,
            ss.getSum(),
            ss.getTps(),
            ss.getAvgpt()));
    }

    public void printAtHour() {
        StatsSnapshot ss = computeStatsData(this.csListHour);
        log.info(String.format("[%s] [%s] Stats In One Hour, SUM: %d TPS: %.2f AVGPT: %.2f",
            this.statsName,
            this.statsKey,
            ss.getSum(),
            ss.getTps(),
            ss.getAvgpt()));
    }

    public void printAtDay() {
        StatsSnapshot ss = computeStatsData(this.csListDay);
        log.info(String.format("[%s] [%s] Stats In One Day, SUM: %d TPS: %.2f AVGPT: %.2f",
            this.statsName,
            this.statsKey,
            ss.getSum(),
            ss.getTps(),
            ss.getAvgpt()));
    }

    public AtomicLong getValue() {
        return value;
    }

    public String getStatsKey() {
        return statsKey;
    }

    public String getStatsName() {
        return statsName;
    }

    public AtomicLong getTimes() {
        return times;
    }
}

class CallSnapshot {
    private final long timestamp;
    private final long times;

    private final long value;

    public CallSnapshot(long timestamp, long times, long value) {
        super();
        this.timestamp = timestamp;
        this.times = times;
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getTimes() {
        return times;
    }

    public long getValue() {
        return value;
    }
}
