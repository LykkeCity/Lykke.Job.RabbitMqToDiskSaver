using System;
using System.Threading.Tasks;
using System.IO;
using Common.Log;
using Lykke.Job.RabbitMqToDiskSaver.Core.Services;

namespace Lykke.Job.RabbitMqToDiskSaver.Services
{
    public class DataToDiskSaver : IDataToDiskSaver
    {
        private const string _timeFormat = "yyyyMMdd-HHmmss-fffffff";

        private readonly ILog _log;
        private readonly string _diskPath;

        public DataToDiskSaver(ILog log, string diskPath)
        {
            _log = log;
            _diskPath = diskPath;

            if (!Directory.Exists(_diskPath))
                Directory.CreateDirectory(_diskPath);
        }

        public async Task SaveDataItemAsync(byte[] data)
        {
            var now = DateTime.UtcNow;
            while (true)
            {
                string filename = now.ToString(_timeFormat) + ".data";
                string filePath = Path.Combine(_diskPath, filename);
                try
                {
                    using (var fileStream = File.Open(filePath, FileMode.CreateNew))
                    {
                        await fileStream.WriteAsync(data, 0, data.Length);
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
    }
}
