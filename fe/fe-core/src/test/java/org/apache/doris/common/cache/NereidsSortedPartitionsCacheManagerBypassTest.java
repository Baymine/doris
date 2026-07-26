// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.common.cache;

import org.apache.doris.catalog.SupportBinarySearchFilteringPartitions;
import org.apache.doris.nereids.rules.expression.rules.SortedPartitionRanges;
import org.apache.doris.nereids.rules.expression.rules.SortedPartitionRanges.PartitionItemAndId;
import org.apache.doris.nereids.rules.expression.rules.SortedPartitionRanges.PartitionItemAndRange;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Pins the per-table partition-count bypass in
 * {@link NereidsSortedPartitionsCacheManager#shouldBypassCache}.
 * <p>
 * The threshold sums {@code sortedPartitions + defaultPartitions} -- both
 * branches occupy heap when cached. Lists hold mock entries because the helper
 * only reads {@code .size()}; element internals are irrelevant.
 */
public class NereidsSortedPartitionsCacheManagerBypassTest {

    @Test
    public void testBelowThresholdNotBypassed() {
        SortedPartitionRanges<Long> ranges = build(/*sorted*/ 100, /*defaults*/ 0);
        Assertions.assertFalse(NereidsSortedPartitionsCacheManager.shouldBypassCache(ranges, 50000));
    }

    @Test
    public void testAtThresholdNotBypassed() {
        // <= threshold stays cached. The threshold is the inclusive upper bound.
        SortedPartitionRanges<Long> ranges = build(50000, 0);
        Assertions.assertFalse(NereidsSortedPartitionsCacheManager.shouldBypassCache(ranges, 50000));
    }

    @Test
    public void testAboveThresholdBypassed() {
        SortedPartitionRanges<Long> ranges = build(50001, 0);
        Assertions.assertTrue(NereidsSortedPartitionsCacheManager.shouldBypassCache(ranges, 50000));
    }

    @Test
    public void testDefaultPartitionsCountedTowardsThreshold() {
        // 49999 regular + 2 default = 50001 -- should bypass.
        SortedPartitionRanges<Long> ranges = build(49999, 2);
        Assertions.assertTrue(NereidsSortedPartitionsCacheManager.shouldBypassCache(ranges, 50000));
    }

    @Test
    public void testZeroThresholdDisablesBypass() {
        // threshold <= 0 means "no limit". Even huge tables stay cached.
        SortedPartitionRanges<Long> ranges = build(1_000_000, 0);
        Assertions.assertFalse(NereidsSortedPartitionsCacheManager.shouldBypassCache(ranges, 0));
    }

    @Test
    public void testNegativeThresholdDisablesBypass() {
        SortedPartitionRanges<Long> ranges = build(1_000_000, 0);
        Assertions.assertFalse(NereidsSortedPartitionsCacheManager.shouldBypassCache(ranges, -1));
    }

    @Test
    public void testCacheOrBypassPutsWhenWithinThreshold() throws Exception {
        // Within-threshold path: the freshly built ranges land in the
        // underlying Caffeine cache and the helper returns them.
        NereidsSortedPartitionsCacheManager mgr = new NereidsSortedPartitionsCacheManager();
        SortedPartitionRanges<Long> ranges = build(10, 0);

        SupportBinarySearchFilteringPartitions table =
                Mockito.mock(SupportBinarySearchFilteringPartitions.class);
        Mockito.when(table.getId()).thenReturn(1234L);
        CatalogRelation scan = Mockito.mock(CatalogRelation.class);
        Mockito.when(table.getPartitionMetaVersion(scan)).thenReturn("v1");

        Object key = newTableIdentifier("c", "d", "t");
        SortedPartitionRanges<?> got = invokeCacheOrBypass(mgr, key, table, scan, ranges, 50000);

        Assertions.assertSame(ranges, got);
        Assertions.assertEquals(1, mgr.getPartitionCaches().asMap().size(),
                "within-threshold entries must be retained in the shared cache");
    }

    @Test
    public void testCacheOrBypassSkipsCacheWhenAboveThreshold() throws Exception {
        // Above-threshold path: ranges are returned for the caller's use but
        // never reach the shared cache, so subsequent queries on other tables
        // are not displaced.
        NereidsSortedPartitionsCacheManager mgr = new NereidsSortedPartitionsCacheManager();
        SortedPartitionRanges<Long> ranges = build(50_001, 0);

        SupportBinarySearchFilteringPartitions table =
                Mockito.mock(SupportBinarySearchFilteringPartitions.class);
        CatalogRelation scan = Mockito.mock(CatalogRelation.class);

        Object key = newTableIdentifier("c", "d", "huge_table");
        SortedPartitionRanges<?> got = invokeCacheOrBypass(mgr, key, table, scan, ranges, 50000);

        Assertions.assertSame(ranges, got, "bypass must still return the ranges to the caller");
        Assertions.assertTrue(mgr.getPartitionCaches().asMap().isEmpty(),
                "above-threshold entries must not be cached");
        // bypass path never touches the table-side meta-version accessor, so
        // the soft-ref pool grows by at most one large entry at a time.
        Mockito.verify(table, Mockito.never()).getPartitionMetaVersion(Mockito.any());
        Mockito.verify(table, Mockito.never()).getId();
    }

    private static Object newTableIdentifier(String catalog, String db, String table) throws Exception {
        Class<?> cls = Class.forName(
                "org.apache.doris.common.cache.NereidsSortedPartitionsCacheManager$TableIdentifier");
        Constructor<?> ctor = cls.getDeclaredConstructor(String.class, String.class, String.class);
        ctor.setAccessible(true);
        return ctor.newInstance(catalog, db, table);
    }

    private static SortedPartitionRanges<?> invokeCacheOrBypass(
            NereidsSortedPartitionsCacheManager mgr, Object key,
            SupportBinarySearchFilteringPartitions table, CatalogRelation scan,
            SortedPartitionRanges<?> ranges, int maxPartitionPerTable) throws Exception {
        Class<?> identClass = Class.forName(
                "org.apache.doris.common.cache.NereidsSortedPartitionsCacheManager$TableIdentifier");
        java.lang.reflect.Method m = NereidsSortedPartitionsCacheManager.class.getDeclaredMethod(
                "cacheOrBypass", identClass, SupportBinarySearchFilteringPartitions.class,
                CatalogRelation.class, SortedPartitionRanges.class, int.class);
        m.setAccessible(true);
        try {
            return (SortedPartitionRanges<?>) m.invoke(mgr, key, table, scan, ranges, maxPartitionPerTable);
        } catch (java.lang.reflect.InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new RuntimeException(cause);
        }
    }

    private static SortedPartitionRanges<Long> build(int sortedCount, int defaultCount) {
        List<PartitionItemAndRange<Long>> sorted = new ArrayList<>(sortedCount);
        @SuppressWarnings("unchecked")
        PartitionItemAndRange<Long> mockRange = Mockito.mock(PartitionItemAndRange.class);
        for (int i = 0; i < sortedCount; i++) {
            sorted.add(mockRange);
        }

        List<PartitionItemAndId<Long>> defaults;
        if (defaultCount == 0) {
            defaults = Collections.emptyList();
        } else {
            @SuppressWarnings("unchecked")
            PartitionItemAndId<Long> mockId = Mockito.mock(PartitionItemAndId.class);
            defaults = new ArrayList<>(defaultCount);
            for (int i = 0; i < defaultCount; i++) {
                defaults.add(mockId);
            }
        }

        return new SortedPartitionRanges<>(sorted, defaults);
    }
}
