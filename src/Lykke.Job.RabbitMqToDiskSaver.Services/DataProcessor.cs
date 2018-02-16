using System;
using System.Linq;
using System.IO;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Lykke.Job.RabbitMqToDiskSaver.Core.Domain.Models;
using Lykke.Job.RabbitMqToDiskSaver.Core.Services;

namespace Lykke.Job.RabbitMqToDiskSaver.Services
{
    public class DataProcessor : TimerPeriod, IDataProcessor
    {
        private const string _directoryFormat = "yyyy-MM-dd-HH";
        private const int _gigabyte = 1024 * 1024 * 1024;

        private readonly ILog _log;
        private readonly IDiskWorker _diskWorker;
        private readonly string _diskPath;
        private readonly int _warningSizeInGigabytes;
        private readonly int _maxSizeInGigabytes;
        private readonly DirectoryInfo _dirInfo;

        public DataProcessor(
            IDiskWorker diskWorker,
            ILog log,
            string diskPath,
            int warningSizeInGigabytes,
            int maxSizeInGigabytes)
            : base((int)TimeSpan.FromMinutes(90).TotalMilliseconds, log)
        {
            _diskWorker = diskWorker;
            _log = log;
            _diskPath = diskPath;
            _warningSizeInGigabytes = warningSizeInGigabytes > 0 ? warningSizeInGigabytes : 0;
            _maxSizeInGigabytes = maxSizeInGigabytes > 0 ? maxSizeInGigabytes : 0;

            if (!Directory.Exists(_diskPath))
                Directory.CreateDirectory(_diskPath);

            _dirInfo = new DirectoryInfo(_diskPath);
            Directory.SetCurrentDirectory(_diskPath);
        }

        public void Process(Orderbook item)
        {
            string directory1 = $"{item.AssetPair}-{(item.IsBuy ? "buy" : "sell")}";
            if (!Directory.Exists(directory1))
                Directory.CreateDirectory(directory1);
            string directory2 = item.Timestamp.ToString(_directoryFormat);
            var dirPath = Path.Combine(directory1, directory2);

            var convertedText = OrderbookConverter.FormatMessage(item);

            _diskWorker.AddDataItem(convertedText, dirPath);
        }

        public override async Task Execute()
        {
            if (_warningSizeInGigabytes == 0 && _maxSizeInGigabytes == 0)
                return;

            var fileInfos = _dirInfo.EnumerateFiles();
            long totalSize = fileInfos.Sum(f => f.Length);
            int gbSize = (int)(totalSize / _gigabyte);

            if (_warningSizeInGigabytes > 0 && gbSize >= _warningSizeInGigabytes)
                await _log.WriteWarningAsync(
                    nameof(DiskWorker),
                    nameof(Execute),
                    $"RabbitMq data on {_diskPath} have taken {gbSize}Gb (>= {_warningSizeInGigabytes}Gb)");

            if (_maxSizeInGigabytes == 0 || gbSize < _maxSizeInGigabytes)
                return;

            long sizeToFree = totalSize - _maxSizeInGigabytes * _gigabyte;
            int deletedFilesCount = 0;
            foreach (var file in fileInfos)
            {
                try
                {
                    if (!File.Exists(file.FullName))
                        continue;
                    File.Delete(file.FullName);
                    sizeToFree -= file.Length;
                    ++deletedFilesCount;
                    if (sizeToFree <= 0)
                        break;
                }
                catch (Exception ex)
                {
                    await _log.WriteWarningAsync(nameof(DiskWorker), nameof(Execute), $"Couldn't delete {file.Name}", ex);
                }
            }
            if (deletedFilesCount > 0)
                await _log.WriteWarningAsync(nameof(DiskWorker), nameof(Execute), $"Deleted {deletedFilesCount} files from {_diskPath}");
        }
    }
}
