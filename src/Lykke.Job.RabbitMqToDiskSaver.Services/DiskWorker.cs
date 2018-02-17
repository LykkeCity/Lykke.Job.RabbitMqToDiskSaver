using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.IO;
using Common;
using Common.Log;
using Lykke.Job.RabbitMqToDiskSaver.Core.Services;

namespace Lykke.Job.RabbitMqToDiskSaver.Services
{
    public class DiskWorker : TimerPeriod, IDiskWorker
    {
        private const string _timeFormat = "yyyyMMdd-HHmmss-fffffff";

        private readonly ILog _log;
        private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);

        private List<byte[]> _items = new List<byte[]>();

        public DiskWorker(ILog log, IShutdownManager shutdownManager)
            : base((int)TimeSpan.FromSeconds(3).TotalMilliseconds, log)
        {
            _log = log;
            shutdownManager.Register(this, 1);
        }

        public void AddDataItem(byte[] data)
        {
            _lock.Wait();
            try
            {
                _items.Add(data);
            }
            finally
            {
                _lock.Release();
            }
        }

        public override void Stop()
        {
            base.Stop();

            Execute().GetAwaiter().GetResult();
        }

        public override async Task Execute()
        {
            if (_items.Count == 0)
                return;

            List<byte[]> batch;
            await _lock.WaitAsync();
            try
            {
                batch = _items;
                _items = new List<byte[]>(batch.Count);
            }
            finally
            {
                _lock.Release();
            }

            foreach (var item in batch)
            {
                await SaveDataItemAsync(item);
            }
        }

        private async Task SaveDataItemAsync(byte[] item)
        {
            var now = DateTime.UtcNow;
            while (true)
            {
                string fileName = now.ToString(_timeFormat) + ".data";
                try
                {
                    using (var fileStream = File.Open(fileName, FileMode.CreateNew))
                    {
                        fileStream.Write(item, 0, item.Length);
                    }
                    break;
                }
                catch (IOException) when (File.Exists(fileName))
                {
                    now = now.AddTicks(1);
                }
                catch (Exception exc)
                {
                    await _log.WriteErrorAsync(nameof(DiskWorker), nameof(SaveDataItemAsync), exc);
                    throw;
                }
            }
        }
    }
}
