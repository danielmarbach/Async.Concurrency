﻿// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.ServiceBus.Primitives
{
    using System;
    using System.Collections.Concurrent;
    using System.Threading.Tasks;

    // Data structure that is used to track LockTokens of deferred messages that are received with
    // messageReceiver.ReceiveDeferredMessageAsync(). Lock tockens for those received messages are tracked
    // together with their corresponding LockUntilUTC datetime in the set. When a user calls CompleteAsync for all the tokens
    // that are tricked in that set a special completion logic is used. Items in the set are expiring based on their expiration date
    // The set has the following problems:
    // - Eventhough it uses conccurent dictionary internally it requires locking which makes it slow
    // - Eventhough the lock is used access to .Keys locks the whole concurrent dictionary which defeats the purpose of ConcurrentDictionary
    // - Delay tasks can leak up to the delayBetweenCleanups time. So creating message receivers can become expensive because of excessive timer usage
    // - Double scheduling with Task.Run for an async operations (increases worker thread pool pressure)
    // - Race between AddOrUpdate and a cleanup interval can remove freshly updated entries because they are seen as outdated by the cleanup loop
    //   currently prevented by the locking semantics of .Keys but could become an issue of the semantics are changed in the framework due to optimizations
    sealed class ConcurrentExpiringSetBefore<TKey>
    {
        readonly ConcurrentDictionary<TKey, DateTime> dictionary;
        readonly object cleanupSynObject = new object();
        bool cleanupScheduled;
        static TimeSpan delayBetweenCleanups = TimeSpan.FromSeconds(10);  // changed from 30 to 10 for demo purposes

        public ConcurrentExpiringSetBefore()
        {
            this.dictionary = new ConcurrentDictionary<TKey, DateTime>();
        }

        public void AddOrUpdate(TKey key, DateTime expiration)
        {
            this.dictionary[key] = expiration;
            Console.Write($"+{key}");
            this.ScheduleCleanup();
        }

        public bool Contains(TKey key)
        {
            return this.dictionary.TryGetValue(key, out var expiration) && expiration > DateTime.UtcNow;
        }

        void ScheduleCleanup()
        {
            lock (this.cleanupSynObject)
            {
                if (this.cleanupScheduled || this.dictionary.Count <= 0)
                {
                    return;
                }

                this.cleanupScheduled = true;
                Task.Run(async () => await this.CollectExpiredEntriesAsync().ConfigureAwait(false));
            }
        }

        async Task CollectExpiredEntriesAsync()
        {
            await Task.Delay(delayBetweenCleanups);

            lock (this.cleanupSynObject)
            {
                this.cleanupScheduled = false;
            }

            foreach (var key in this.dictionary.Keys)
            {
                if (DateTime.UtcNow > this.dictionary[key])
                {
                    this.dictionary.TryRemove(key, out _);
                    Console.Write($"-{key}");
                }
            }

            this.ScheduleCleanup();
        }
    }
}