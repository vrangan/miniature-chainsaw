/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.miniature.chainsaw.snapshot.retention.util;

import java.util.Calendar;

/**
 * TimeUnit used for Date operations.
 */
public enum CalendarUnit {
    MINUTE(Calendar.MINUTE), HOUR(Calendar.HOUR), DAY(Calendar.DATE), MONTH(Calendar.MONTH),
    YEAR(Calendar.YEAR), END_OF_DAY(Calendar.DATE), END_OF_MONTH(Calendar.MONTH), CRON(0), NONE(-1);

    private int calendarUnit;

    private CalendarUnit(int calendarUnit) {
        this.calendarUnit = calendarUnit;
    }

    public int getCalendarUnit() {
        return calendarUnit;
    }
}
