/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package dev.morling.onebrc;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.IntStream;

import sun.misc.Unsafe;

public class CalculateAverage_abeobk {
    private static final boolean SHOW_ANALYSIS = false;

    private static final String FILE = "./measurements.txt";
    private static final int BUCKET_SIZE = 1 << 16;
    private static final int BUCKET_MASK = BUCKET_SIZE - 1;
    private static final int MAX_STR_LEN = 100;
    private static final int MAX_STATIONS = 10000;
    private static final Unsafe UNSAFE = initUnsafe();
    private static final long[] HASH_MASKS = new long[]{
            0x0L,
            0xffL,
            0xffffL,
            0xffffffL,
            0xffffffffL,
            0xffffffffffL,
            0xffffffffffffL,
            0xffffffffffffffL,
            0xffffffffffffffffL, };

    private static final void debug(String s, Object... args) {
        System.out.println(String.format(s, args));
    }

    private static Unsafe initUnsafe() {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            return (Unsafe) theUnsafe.get(Unsafe.class);
        }
        catch (Exception ex) {
            throw new RuntimeException();
        }
    }

    static class Stat {
        long sum;
        int count;
        short min, max;
        String key;

        Stat(String k, long s, int c, short m, short mx) {
            key = k;
            sum = s;
            count = c;
            min = m;
            max = mx;
        }

        public final String toString() {
            return (min / 10.0) + "/"
                    + (Math.round(((double) sum / count)) / 10.0) + "/"
                    + (max / 10.0);
        }

        final void merge(Stat other) {
            sum += other.sum;
            count += other.count;
            if (other.max > max)
                max = other.max;
            if (other.min < min)
                min = other.min;
        }
    }

    static class Table {
        static final int LEN = 0;
        static final int CNT = LEN + 4;
        static final int SUM = CNT + 4;
        static final int MAX = SUM + 8;
        static final int MIN = MAX + 2;
        static final int STR = MIN + 2;
        static final int ROW_SZ = STR + 104;
        static final int TBL_SZ = ROW_SZ * (BUCKET_SIZE + MAX_STATIONS);

        final long base_addr;
        final long end_addr;
        long row_addr;

        Table() {
            // artsiomkorzun aligned address to page size (default=4096)
            base_addr = (UNSAFE.allocateMemory(TBL_SZ + 4096) + 4095) & (~4095);
            // base_addr = UNSAFE.allocateMemory(TBL_SZ);
            UNSAFE.setMemory(base_addr, TBL_SZ, (byte) 0);
            end_addr = base_addr + TBL_SZ;
        }

        final Stat getStat() {
            int keylen = len();
            if (keylen == 0)
                return null;
            byte[] sbuf = new byte[MAX_STR_LEN];
            UNSAFE.copyMemory(null, row_addr + STR, sbuf, Unsafe.ARRAY_BYTE_BASE_OFFSET, keylen);
            return new Stat(new String(sbuf, 0, keylen, StandardCharsets.UTF_8), sum(), count(), min(), max());
        }

        final void setRow(int row_idx) {
            row_addr = base_addr + row_idx * ROW_SZ;
        }

        final boolean hasRow() {
            return row_addr < end_addr;
        }

        final void nextRow() {
            row_addr += ROW_SZ;
        }

        final void put(long a, short v, int kl) {
            UNSAFE.putInt(row_addr + LEN, kl);
            UNSAFE.putLong(row_addr + SUM, v);
            UNSAFE.putInt(row_addr + CNT, 1);
            UNSAFE.putShort(row_addr + MIN, v);
            UNSAFE.putShort(row_addr + MAX, v);
            UNSAFE.copyMemory(a, row_addr + STR, kl);
        }

        final void add(short v) {
            UNSAFE.putLong(row_addr + SUM, UNSAFE.getLong(row_addr + SUM) + v);
            UNSAFE.putInt(row_addr + CNT, UNSAFE.getInt(row_addr + CNT) + 1);
            if (v >= UNSAFE.getShort(row_addr + MAX)) {
                UNSAFE.putShort(row_addr + MAX, v);
            }
            else if (v < UNSAFE.getShort(row_addr + MIN)) {
                UNSAFE.putShort(row_addr + MIN, v);
            }
        }

        final int len() {
            return UNSAFE.getInt(row_addr + LEN);
        }

        final long word(int woff) {
            return UNSAFE.getLong(row_addr + STR + woff);
        }

        final long wordAt(long addr) {
            return UNSAFE.getLong(addr);
        }

        final long sum() {
            return UNSAFE.getLong(row_addr + SUM);
        }

        final int count() {
            return UNSAFE.getInt(row_addr + CNT);
        }

        final short min() {
            return UNSAFE.getShort(row_addr + MIN);
        }

        final short max() {
            return UNSAFE.getShort(row_addr + MAX);
        }

        final boolean crrRowEmpty() {
            return UNSAFE.getShort(row_addr) == 0;
        }

        // key<8
        final boolean match(long tail) {
            return word(0) == tail;
        }

        // key<16
        final boolean match(long word0, long tail) {
            long sptr = row_addr + STR;
            return wordAt(sptr) == word0 && wordAt(sptr + 8) == tail;
        }

        // key<24
        final boolean match(long word0, long word1, long tail) {
            long sptr = row_addr + STR;
            return wordAt(sptr) == word0 && wordAt(sptr + 8) == word1 && wordAt(sptr + 16) == tail;
        }

        // key>24
        final boolean match(long addr, long word0, long word1, long tail, int keylen) {
            int n = keylen & 0xF8;
            long sptr = row_addr + STR;
            if (wordAt(sptr) != word0 || wordAt(sptr + 8) != word1 || wordAt(sptr + n) != tail)
                return false;
            // this is faster than comparision if key is short
            long xsum = 0;
            for (int i = 16; i < n; i += 8) {
                xsum |= wordAt(sptr + i) ^ UNSAFE.getLong(addr + i);
            }
            return xsum == 0;
        }
    }

    // split into chunks
    static long[] slice(long start_addr, long end_addr, long chunk_size, int cpu_cnt) {
        long[] ptrs = new long[cpu_cnt + 1];
        ptrs[0] = start_addr;
        for (int i = 1; i < cpu_cnt; i++) {
            long addr = start_addr + i * chunk_size;
            while (addr < end_addr && UNSAFE.getByte(addr++) != '\n')
                ;
            ptrs[i] = Math.min(addr, end_addr);
        }
        ptrs[cpu_cnt] = end_addr;
        return ptrs;
    }

    // idea from royvanrijn
    static final long getSemiPosCode(final long word) {
        long xor_semi = word ^ 0x3b3b3b3b3b3b3b3bL; // xor with ;;;;;;;;
        return (xor_semi - 0x0101010101010101L) & (~xor_semi & 0x8080808080808080L);
    }

    static final int mix(long hash) {
        long h = hash * 37;
        return ((int) (h ^ (h >>> 29))) & BUCKET_MASK;
    }

    // great idea from merykitty (Quan Anh Mai)
    static final short parseNum(long num_word, int dot_pos) {
        int shift = 28 - dot_pos;
        long signed = (~num_word << 59) >> 63;
        long dsmask = ~(signed & 0xFF);
        long digits = ((num_word & dsmask) << shift) & 0x0F000F0F00L;
        long abs_val = ((digits * 0x640a0001) >>> 32) & 0x3FF;
        return (short) ((abs_val ^ signed) - signed);
    }

    // optimize for contest
    // save as much slow memory access as possible
    // about 50% key < 8chars, 25% key bettween 8-10 chars
    // keylength histogram (%) = [0, 0, 0, 0, 4, 10, 21, 15, 13, 11, 6, 6, 4, 2...
    static final List<Stat> parse(int thread_id, long start, long end) {
        int cls = 0;
        long addr = start;
        Table table = new Table();
        // parse loop
        while (addr < end) {
            long row_addr = addr;
            long word0 = UNSAFE.getLong(addr);
            long semipos_code = getSemiPosCode(word0);

            // about 50% chance key < 8 chars
            if (semipos_code != 0) {
                int semi_pos = Long.numberOfTrailingZeros(semipos_code) >>> 3;
                addr += semi_pos + 1;
                long num_word = UNSAFE.getLong(addr);
                int dot_pos = Long.numberOfTrailingZeros(~num_word & 0x10101000);
                addr += (dot_pos >>> 3) + 3;

                long tail = word0 & HASH_MASKS[semi_pos];
                short val = parseNum(num_word, dot_pos);
                table.setRow(mix(tail));

                while (true) {
                    if (table.crrRowEmpty()) {
                        table.put(row_addr, val, semi_pos);
                        break;
                    }
                    if (table.match(tail)) {
                        table.add(val);
                        break;
                    }
                    table.nextRow();
                    if (SHOW_ANALYSIS)
                        cls++;
                }
                continue;
            }

            addr += 8;
            long word1 = UNSAFE.getLong(addr);
            semipos_code = getSemiPosCode(word1);
            // 43% chance
            if (semipos_code != 0) {
                int semi_pos = Long.numberOfTrailingZeros(semipos_code) >>> 3;
                addr += semi_pos + 1;
                long num_word = UNSAFE.getLong(addr);
                int dot_pos = Long.numberOfTrailingZeros(~num_word & 0x10101000);
                addr += (dot_pos >>> 3) + 3;

                long tail = (word1 & HASH_MASKS[semi_pos]);
                short val = parseNum(num_word, dot_pos);
                table.setRow(mix(word0 ^ tail));

                while (true) {
                    if (table.crrRowEmpty()) {
                        table.put(row_addr, val, 8 + semi_pos);
                        break;
                    }
                    if (table.match(word0, tail)) {
                        table.add(val);
                        break;
                    }
                    table.nextRow();
                    if (SHOW_ANALYSIS)
                        cls++;
                }
                continue;
            }

            long hash = word0 ^ word1;
            addr += 8;
            long word = UNSAFE.getLong(addr);
            semipos_code = getSemiPosCode(word);

            if (semipos_code != 0) {
                int semi_pos = Long.numberOfTrailingZeros(semipos_code) >>> 3;
                addr += semi_pos + 1;
                long num_word = UNSAFE.getLong(addr);
                int dot_pos = Long.numberOfTrailingZeros(~num_word & 0x10101000);
                addr += (dot_pos >>> 3) + 3;

                long tail = (word & HASH_MASKS[semi_pos]);
                short val = parseNum(num_word, dot_pos);
                table.setRow(mix(hash ^ tail));

                while (true) {
                    if (table.crrRowEmpty()) {
                        table.put(row_addr, val, 16 + semi_pos);
                        break;
                    }
                    if (table.match(word0, word1, tail)) {
                        table.add(val);
                        break;
                    }
                    table.nextRow();
                    if (SHOW_ANALYSIS)
                        cls++;
                }
                continue;
            }

            while (semipos_code == 0) {
                hash ^= word;
                addr += 8;
                word = UNSAFE.getLong(addr);
                semipos_code = getSemiPosCode(word);
            }

            int semi_pos = Long.numberOfTrailingZeros(semipos_code) >>> 3;
            addr += semi_pos;
            int keylen = (int) (addr - row_addr);
            long num_word = UNSAFE.getLong(addr + 1);
            int dot_pos = Long.numberOfTrailingZeros(~num_word & 0x10101000);
            addr += (dot_pos >>> 3) + 4;

            long tail = (word & HASH_MASKS[semi_pos]);
            short val = parseNum(num_word, dot_pos);
            table.setRow(mix(hash ^ tail));

            while (true) {
                if (table.crrRowEmpty()) {
                    table.put(row_addr, val, keylen);
                    break;
                }
                if (table.match(row_addr, word0, word1, tail, keylen)) {
                    table.add(val);
                    break;
                }
                table.nextRow();
                if (SHOW_ANALYSIS)
                    cls++;
            }
        }

        if (SHOW_ANALYSIS) {
            debug("Thread %d collision = %d", thread_id, cls);
        }
        List<Stat> stats = new ArrayList<>(MAX_STATIONS);

        table.setRow(0);
        while (table.hasRow()) {
            Stat s = table.getStat();
            if (s != null) {
                stats.add(s);
            }
            table.nextRow();
        }
        return stats;
    }

    // thomaswue trick
    private static void spawnWorker() throws IOException {
        ProcessHandle.Info info = ProcessHandle.current().info();
        ArrayList<String> workerCommand = new ArrayList<>();
        info.command().ifPresent(workerCommand::add);
        info.arguments().ifPresent(args -> workerCommand.addAll(Arrays.asList(args)));
        workerCommand.add("--worker");
        new ProcessBuilder()
                .command(workerCommand)
                .start()
                .getInputStream()
                .transferTo(System.out);
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        // thomaswue trick
        if (args.length == 0 || !("--worker".equals(args[0]))) {
            spawnWorker();
            return;
        }

        try (var file = FileChannel.open(Path.of(FILE), StandardOpenOption.READ)) {
            long start_addr = file.map(MapMode.READ_ONLY, 0, file.size(), Arena.global()).address();
            long file_size = file.size();
            long end_addr = start_addr + file_size;

            // only use all cpus on large file
            int cpu_cnt = file_size < 1e6 ? 1 : Runtime.getRuntime().availableProcessors();
            long chunk_size = Math.ceilDiv(file_size, cpu_cnt);

            // processing
            var ptrs = slice(start_addr, end_addr, chunk_size, cpu_cnt);

            List<List<Stat>> maps = IntStream.range(0, cpu_cnt)
                    .mapToObj(thread_id -> parse(thread_id, ptrs[thread_id], ptrs[thread_id + 1]))
                    .parallel()
                    .toList();

            TreeMap<String, Stat> ms = new TreeMap<>();
            for (var stats : maps) {
                for (var s : stats) {
                    var stat = ms.putIfAbsent(s.key, s);
                    if (stat != null)
                        stat.merge(s);
                }
            }

            // print result
            System.out.println(ms);
            System.out.close();
        }
    }
}