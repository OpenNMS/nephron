/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2021 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2021 The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is a registered trademark of The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * OpenNMS(R) is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with OpenNMS(R).  If not, see:
 *      http://www.gnu.org/licenses/
 *
 * For more information contact:
 *     OpenNMS(R) Licensing <license@opennms.org>
 *     http://www.opennms.org/
 *     http://www.opennms.com/
 *******************************************************************************/

package org.opennms.nephron;

/**
 * Explicit congestion notification information.
 *
 * ECN information is encoded with 2 bits i.e. it could represent 4 different states (i.e. code 0, 1, 2, 3). However,
 * only 3 different semantic states are distinguished. Both, code 1 and 2 both indicate an ecn capable transport.
 * In order to save space when grouping flows according to their ecn information, code 2 is treated as code 1.
 */
public enum Ecn {

    NON_ECT(0), // non ecn-capable transport
    ECT(1), // ecn-capable transport
    CE(3); // congestion encountered

    public final int code;

    Ecn(int code) {
        this.code = code;
    }
}
