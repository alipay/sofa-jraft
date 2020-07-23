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
package com.alipay.sofa.jraft.test.atomic;

import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class HashUtils {

    public static long getHeadKey(Map<Long, ?> map, String key) {
        TreeMap<Long, ?> treeMap = new TreeMap(map);
        long hash = Math.abs(HashAlgorithm.FNV1_64_HASH.hash(key));
        SortedMap<Long, ?> headMap = treeMap.headMap(hash, true);
        return headMap != null && !headMap.isEmpty() ? headMap.lastKey() : treeMap.firstKey();
    }
}
