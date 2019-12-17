using System;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus.Primitives;

namespace Async.Concurrency
{
    class Program
    {
        static void Main(string[] args)
        {
            var utcNow = DateTime.UtcNow;
            Console.WriteLine("Old Expiry Set");
            var oldSet = new ConcurrentExpiringSetBefore<int>();
            Parallel.For(0, 20000, i => { oldSet.AddOrUpdate(i, utcNow.AddMilliseconds(i)); });
            Console.ReadLine();
            
            Console.WriteLine("New Expiry Set");
            var newSet = new ConcurrentExpiringSetAfter<int>();
            Parallel.For(0, 20000, i => { newSet.AddOrUpdate(i, utcNow.AddMilliseconds(i)); });

            Console.ReadLine();
        }
    }
}
