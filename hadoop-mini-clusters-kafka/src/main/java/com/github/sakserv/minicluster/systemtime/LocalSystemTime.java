/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.github.sakserv.minicluster.systemtime;

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.Time;

import java.util.function.Supplier;

public class LocalSystemTime implements Time {

    @Override
    public long milliseconds() {
        return System.currentTimeMillis();
    }

    @Override
    public long nanoseconds() {
        return System.nanoTime();
    }

    @Override
    public void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            // no stress
        }
    }

    @Override
    public void waitObject(final Object object, final Supplier<Boolean> condition, final long deadlineMs) throws InterruptedException {
        //noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized(object) {
            while(!condition.get()) {
                long currentTimeMs = this.milliseconds();
                if (currentTimeMs >= deadlineMs) {
                    throw new TimeoutException("Condition not satisfied before deadline");
                }

                object.wait(deadlineMs - currentTimeMs);
            }
        }
    }

    @Override
    public long hiResClockMs() {
        return System.currentTimeMillis();
    }

}