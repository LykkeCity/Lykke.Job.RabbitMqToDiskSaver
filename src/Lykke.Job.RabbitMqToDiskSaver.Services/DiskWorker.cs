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
        private const string _dateFormat = "yyyy-MM-dd";
        private const string _hourFormat = "yyyy-MM-dd-HH";

        private readonly ILog _log;
        private readonly bool _isHourlyBatched;
        private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);

        private List<byte[]> _items = new List<byte[]>();
        private DateTime _lastAddedTime = DateTime.MinValue;

        public DiskWorker(
            ILog log,
            IShutdownManager shutdownManager,
            bool isHourlyBatched)
            : base((int)TimeSpan.FromSeconds(3).TotalMilliseconds, log)
        {
            _log = log;
            _isHourlyBatched = isHourlyBatched;

            shutdownManager.Register(this, 1);
        }

        public void AddDataItem(byte[] data)
        {
            var now = DateTime.UtcNow;
            List<byte[]> batch = null;
            DateTime? batchTime = null;
            _lock.Wait();
            try
            {
                if (_lastAddedTime.Date != now.Date || _isHourlyBatched && (_lastAddedTime.Hour != now.Hour))
                {
                    if (_items.Count > 0)
                    {
                        batch = _items;
                        _items = new List<byte[]>(batch.Count);
                        batchTime = _lastAddedTime;
                    }
                    _lastAddedTime = now;
                }
                _items.Add(data);
            }
            finally
            {
                _lock.Release();
            }
            if (batch != null)
                Task.Run(() => SaveBatchAsync(batch, batchTime.Value).GetAwaiter().GetResult());
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

            await SaveBatchAsync(batch, _lastAddedTime);
        }

        private async Task SaveBatchAsync(List<byte[]> batch, DateTime batchDate)
        {
            string batchDirectory = batchDate.ToString(_isHourlyBatched ? _hourFormat : _dateFormat);
            if (!Directory.Exists(batchDirectory))
                Directory.CreateDirectory(batchDirectory);
            foreach (var item in batch)
            {
                await SaveDataItemAsync(item, batchDirectory);
            }
        }

        private async Task SaveDataItemAsync(byte[] item, string directory)
        {
            var now = DateTime.UtcNow;
            while (true)
            {
                string fileName = now.ToString(_timeFormat) + ".data";
                string filePath = Path.Combine(directory, fileName);
                try
                {
                    using (var fileStream = File.Open(filePath, FileMode.CreateNew))
                    {
                        fileStream.Write(item, 0, item.Length);
                    }
                    break;
                }
                catch (IOException) when (File.Exists(filePath))
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
