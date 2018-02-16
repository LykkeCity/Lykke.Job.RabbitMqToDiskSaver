using System;
using System.Linq;
using System.Threading.Tasks;
using System.IO;
using Common;
using Common.Log;
using Lykke.Job.RabbitMqToDiskSaver.Core.Services;
using Lykke.Job.RabbitMqToDiskSaver.Core.Domain.Models;

namespace Lykke.Job.RabbitMqToDiskSaver.Services
{
    public class DataToDiskSaver : TimerPeriod, IDataToDiskSaver
    {
        private const string _timeFormat = "yyyyMMdd-HHmmss-fffffff";
        private const string _directoryFormat = "yyyy-MM-dd-HH";
        private const int _gigabyte = 1024 * 1024 * 1024;

        private readonly ILog _log;
        private readonly string _diskPath;
        private readonly int _warningSizeInGigabytes;
        private readonly int _maxSizeInGigabytes;
        private readonly DirectoryInfo _dirInfo;

        public DataToDiskSaver(
            ILog log,
            string diskPath,
            int warningSizeInGigabytes,
            int maxSizeInGigabytes)
            : base((int)TimeSpan.FromMinutes(1).TotalMilliseconds, log)
        {
            _log = log;
            _diskPath = diskPath;
            _warningSizeInGigabytes = warningSizeInGigabytes > 0 ? warningSizeInGigabytes : 0;
            _maxSizeInGigabytes = maxSizeInGigabytes > 0 ? maxSizeInGigabytes : 0;

            if (!Directory.Exists(_diskPath))
                Directory.CreateDirectory(_diskPath);

            _dirInfo = new DirectoryInfo(_diskPath);
            Directory.SetCurrentDirectory(_diskPath);
        }

        public async Task SaveDataItemAsync(Orderbook item)
        {
            string directory1 = $"{item.AssetPair}_{(item.IsBuy ? "-buy" : "-sell")}";
            if (!Directory.Exists(directory1))
                Directory.CreateDirectory(directory1);
            var now = DateTime.UtcNow;
            string directory2 = now.ToString(_directoryFormat);
            var dirPath = Path.Combine(directory1, directory2);
            if (!Directory.Exists(dirPath))
                Directory.CreateDirectory(dirPath);

            var convertedText = OrderbookConverter.FormatMessage(item);

            while (true)
            {
                string fileName = now.ToString(_timeFormat) + ".data";
                string filePath = Path.Combine(dirPath, fileName);
                try
                {
                    using (var fileStream = File.Open(filePath, FileMode.CreateNew))
                    {
                        fileStream.WriteString(convertedText);
                    }
                    break;
                }
                catch (IOException) when (File.Exists(filePath))
                {
                    now = now.AddTicks(1);
                }
                catch (Exception exc)
                {
                    await _log.WriteErrorAsync(nameof(DataToDiskSaver), nameof(SaveDataItemAsync), exc);
                    throw;
                }
            }
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
                    nameof(DataToDiskSaver),
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
                    await _log.WriteWarningAsync(nameof(DataToDiskSaver), nameof(Execute), $"Couldn't delete {file.Name}", ex);
                }
            }
            if (deletedFilesCount > 0)
                await _log.WriteWarningAsync(nameof(DataToDiskSaver), nameof(Execute), $"Deleted {deletedFilesCount} files from {_diskPath}");
        }
    }
}
