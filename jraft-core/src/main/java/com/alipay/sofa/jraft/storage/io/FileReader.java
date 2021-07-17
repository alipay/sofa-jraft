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
package com.alipay.sofa.jraft.storage.io;

import java.io.IOException;

import com.alipay.sofa.jraft.error.RetryAgainException;
import com.alipay.sofa.jraft.util.ByteBufferCollector;

/**
 * Read data from a file, all the method should be thread-safe.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-13 11:48:34 AM
 */
public interface FileReader {

    int EOF = -1;

    /**
     * Get the slice size.
     *
     * @return slice size of the file
     */
    long getSliceSize();

    /**
     * Get the file path.
     *
     * @return path of the file
     */
    String getPath();

    /**
     * Read file into buf starts from offset at most maxCount.
     *
     * @param buf      read bytes into this buf
     * @param fileName file name
     * @param offset   the offset of file
     * @param maxCount max read bytes
     * @return -1 if reaches end, else return read count.
     * @throws IOException if some I/O error occurs
     * @throws RetryAgainException if it's not allowed to read partly
     * or it's allowed but throughput is throttled to 0, try again.
     */
    int readFile(final ByteBufferCollector buf, final String fileName, final long sliceId, final long offset,
                 final long maxCount) throws IOException, RetryAgainException;
}
