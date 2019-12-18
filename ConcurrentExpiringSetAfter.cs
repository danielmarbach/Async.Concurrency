// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.ServiceBus.Primitives
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Concurrent;
    using System.Threading;
    using System.Threading.Tasks;

#region You gotta have these
    // Data structure that is used to track LockTokens of deferred messages that are received with
    // messageReceiver.ReceiveDeferredMessageAsync(). Lock tockens for those received messages are tracked
    // together with their corresponding LockUntilUTC datetime in the set. When a user calls CompleteAsync for all the tokens
    // that are tricked in that set a special completion logic is used. Items in the set are expiring based on their expiration date
    // The set has the following advantages over the previous implementation:
    // - No double scheduling due to pure async
    // - No more leaking of delayed tasks when Closing the set which allows aligning the set with the lifetime of the receiver
    // - Enumerating the concurrent dictionary is safe and doesn't acquire a full lock
    // - If an item is due to be removed and is updated while the cleanup thread tries to remove it a snapshot value is used by going through the collection interface
    // Caveat (but highly unlikely):
    // Due to the field exchange, we might miss a TrySetResult on the old TCS and therefore would not run the cleanup again until the next add or update comes. Another contributing factor of this being unlikely is the fact that async statemachinery acquires a full fence semantics by design
    // See PR https://github.com/Azure/azure-sdk-for-net/pull/6577
#endregion
    sealed class ConcurrentExpiringSetAfter<TKey>
    {
        readonly ConcurrentDictionary<TKey, DateTime> dictionary;
        readonly ICollection<KeyValuePair<TKey, DateTime>> dictionaryAsCollection;
        readonly CancellationTokenSource tokenSource = new CancellationTokenSource();
        volatile TaskCompletionSource<bool> cleanupTaskCompletionSource = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        int closeSignaled;
        bool closed;
        static readonly TimeSpan delayBetweenCleanups = TimeSpan.FromSeconds(10); // changed from 30 to 10 for demo purposes

        public ConcurrentExpiringSetAfter() {
            this.dictionary = new ConcurrentDictionary<TKey, DateTime>();
            this.dictionaryAsCollection = dictionary;
            _ = CollectExpiredEntriesAsync(tokenSource.Token);
        }

        public void AddOrUpdate(TKey key, DateTime expiration) {
            this.ThrowIfClosed();

            this.dictionary[key] = expiration;
            Console.Write($"+{key}");
            this.cleanupTaskCompletionSource.TrySetResult(true);
        }

        public bool Contains(TKey key) {
            this.ThrowIfClosed();

            return this.dictionary.TryGetValue(key, out var expiration) && expiration > DateTime.UtcNow;
        }

        public void Close() {
            if (Interlocked.Exchange(ref this.closeSignaled, 1) != 0) {
                return;
            }

            this.closed = true;

            this.tokenSource.Cancel();
            this.cleanupTaskCompletionSource.TrySetCanceled();
            this.dictionary.Clear();
            this.tokenSource.Dispose();
        }

        async Task CollectExpiredEntriesAsync(CancellationToken token) {
            while (!token.IsCancellationRequested) {
                try {
                    await this.cleanupTaskCompletionSource.Task.ConfigureAwait(false);
                    await Task.Delay(delayBetweenCleanups, token).ConfigureAwait(false);
                } catch (OperationCanceledException) {
                    return;
                }

                var isEmpty = true;
                var utcNow = DateTime.UtcNow;
                foreach (var kvp in this.dictionary) {
                    isEmpty = false;
                    var expiration = kvp.Value;
                    if (utcNow > expiration) {
                        this.dictionaryAsCollection.Remove(kvp);
                        Console.Write($"-{kvp.Key}");
                    }
                }

                if (isEmpty) {
                    this.cleanupTaskCompletionSource = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                }
            }
        }

        void ThrowIfClosed()
        {
            if (closed)
            {
                throw new ObjectDisposedException($"ConcurrentExpiringSet has already been closed. Please create a new set instead.");
            }
        }
    }
}