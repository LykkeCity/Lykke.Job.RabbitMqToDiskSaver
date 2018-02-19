using System;
using System.Linq;
using System.IO;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Lykke.Job.RabbitMqToDiskSaver.Core.Services;

namespace Lykke.Job.RabbitMqToDiskSaver.Services
{
    public class DataProcessor : TimerPeriod, IDataProcessor
    {
        private const int _gigabyte = 1024 * 1024 * 1024;

        private readonly ILog _log;
        private readonly IDiskWorker _diskWorker;
        private readonly string _directory;
        private readonly int _warningSizeInGigabytes;
        private readonly int _maxSizeInGigabytes;
        private readonly DirectoryInfo _dirInfo;

        public DataProcessor(
            IDiskWorker diskWorker,
            ILog log,
            IShutdownManager shutdownManager,
            string diskPath,
            string directory,
            int warningSizeInGigabytes,
            int maxSizeInGigabytes)
            : base((int)TimeSpan.FromMinutes(90).TotalMilliseconds, log)
        {
            _diskWorker = diskWorker;
            _log = log;
            _warningSizeInGigabytes = warningSizeInGigabytes > 0 ? warningSizeInGigabytes : 0;
            _maxSizeInGigabytes = maxSizeInGigabytes > 0 ? maxSizeInGigabytes : 0;
            shutdownManager.Register(this, 3);

            _directory = Path.Combine(diskPath, directory);

            if (!Directory.Exists(_directory))
                Directory.CreateDirectory(_directory);

            _dirInfo = new DirectoryInfo(_directory);
            Directory.SetCurrentDirectory(_directory);
        }

        public void Process(byte[] data)
        {
            _diskWorker.AddDataItem(data);
        }

        public override async Task Execute()
        {
            if (_warningSizeInGigabytes == 0 && _maxSizeInGigabytes == 0)
                return;

            var fileInfos = _dirInfo.EnumerateFiles("", SearchOption.AllDirectories);
            long totalSize = fileInfos.Sum(f => f.Length);
            int gbSize = (int)(totalSize / _gigabyte);

            if (_warningSizeInGigabytes > 0 && gbSize >= _warningSizeInGigabytes)
                await _log.WriteWarningAsync(
                    nameof(DataProcessor),
                    nameof(Execute),
                    $"RabbitMq data on {_directory} have taken {gbSize}Gb (>= {_warningSizeInGigabytes}Gb)");

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
                    await _log.WriteWarningAsync(nameof(DataProcessor), nameof(Execute), $"Couldn't delete {file.Name}", ex);
                }
            }
            if (deletedFilesCount > 0)
                await _log.WriteWarningAsync(nameof(DataProcessor), nameof(Execute), $"Deleted {deletedFilesCount} files from {_directory}");
        }
    }
}
