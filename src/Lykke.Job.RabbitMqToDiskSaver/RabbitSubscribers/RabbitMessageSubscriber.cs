using System;
using System.Threading.Tasks;
using Autofac;
using Common;
using Common.Log;
using Lykke.RabbitMqBroker;
using Lykke.RabbitMqBroker.Subscriber;
using Lykke.Job.RabbitMqToDiskSaver.Core.Services;

namespace Lykke.Job.RabbitMqToDiskSaver.RabbitSubscribers
{
    public class RabbitMessageSubscriber : IStartable, IStopable, IMessageDeserializer<byte[]>
    {
        private readonly ILog _log;
        private readonly IConsole _console;
        private readonly IDataProcessor _dataProcessor;
        private readonly string _connectionString;
        private readonly string _exchangeName;
        private RabbitMqSubscriber<byte[]> _subscriber;

        public RabbitMessageSubscriber(
            ILog log,
            IConsole console,
            IDataProcessor dataProcessor,
            IShutdownManager shutdownManager,
            string connectionString,
            string exchangeName)
        {
            _log = log;
            _console = console;
            _dataProcessor = dataProcessor;
            _connectionString = connectionString;
            _exchangeName = exchangeName;
            shutdownManager.Register(this, 0);
        }

        public void Start()
        {
            var settings = RabbitMqSubscriptionSettings
                .CreateForSubscriber(_connectionString, _exchangeName, "rabbitmqtodisksaver")
                .MakeDurable();

            _subscriber = new RabbitMqSubscriber<byte[]>(settings,
                    new ResilientErrorHandlingStrategy(_log, settings,
                        retryTimeout: TimeSpan.FromSeconds(10),
                        next: new DeadQueueErrorHandlingStrategy(_log, settings)))
                .SetMessageDeserializer(this)
                .Subscribe(ProcessMessageAsync)
                .CreateDefaultBinding()
                .SetLogger(_log)
                .SetConsole(_console)
                .Start();
        }

        private async Task ProcessMessageAsync(byte[] item)
        {
            try
            {
                _dataProcessor.Process(item);
            }
            catch (Exception ex)
            {
                await _log.WriteErrorAsync(nameof(RabbitMessageSubscriber), nameof(ProcessMessageAsync), ex);
                throw;
            }
        }

        public void Dispose()
        {
            _subscriber?.Dispose();
        }

        public void Stop()
        {
            _subscriber?.Stop();
        }

        public byte[] Deserialize(byte[] data)
        {
            return data;
        }
    }
}
