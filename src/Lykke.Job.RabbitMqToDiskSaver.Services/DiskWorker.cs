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

        private Dictionary<string, List<string>> _directoriesDict = new Dictionary<string, List<string>>();

        public DiskWorker(ILog log, IShutdownManager shutdownManager)
            : base((int)TimeSpan.FromSeconds(3).TotalMilliseconds, log)
        {
            _log = log;
            shutdownManager.Register(this);
        }

        public void AddDataItem(string text, string directoryPath)
        {
            _lock.Wait();
            try
            {
                if (_directoriesDict.ContainsKey(directoryPath))
                    _directoriesDict[directoryPath].Add(text);
                else
                    _directoriesDict.Add(directoryPath, new List<string> { text });
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
            if (_directoriesDict.Count == 0)
                return;

            Dictionary<string, List<string>> batch;
            await _lock.WaitAsync();
            try
            {
                batch = _directoriesDict;
                _directoriesDict = new Dictionary<string, List<string>>();
            }
            finally
            {
                _lock.Release();
            }

            foreach (var pair in batch)
            {
                await SaveDataItemAsync(pair.Value, pair.Key);
            }
        }

        private async Task SaveDataItemAsync(IEnumerable<string> items, string directoryPath)
        {
            if (!Directory.Exists(directoryPath))
                Directory.CreateDirectory(directoryPath);
            var now = DateTime.UtcNow;
            while (true)
            {
                string fileName = now.ToString(_timeFormat) + ".data";
                string filePath = Path.Combine(directoryPath, fileName);
                try
                {
                    using (var fileStream = File.Open(filePath, FileMode.CreateNew))
                    {
                        using (var witer = new StreamWriter(fileStream))
                        {
                            foreach (var item in items)
                            {
                                await witer.WriteLineAsync(item);
                            }
                        }
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
