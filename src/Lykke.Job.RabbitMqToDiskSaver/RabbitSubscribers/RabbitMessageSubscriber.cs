using System;
using System.Threading.Tasks;
using Autofac;
using Common;
using Common.Log;
using Lykke.RabbitMqBroker;
using Lykke.RabbitMqBroker.Subscriber;
using Lykke.Job.RabbitMqToDiskSaver.Core.Services;
using Lykke.Job.RabbitMqToDiskSaver.Core.Domain.Models;

namespace Lykke.Job.RabbitMqToDiskSaver.RabbitSubscribers
{
    public class RabbitMessageSubscriber : IStartable, IStopable, IMessageDeserializer<byte[]>
    {
        private readonly ILog _log;
        private readonly IConsole _console;
        private readonly IDataProcessor _dataProcessor;
        private readonly string _connectionString;
        private readonly string _exchangeName;
        private RabbitMqSubscriber<Orderbook> _subscriber;

        public RabbitMessageSubscriber(
            ILog log,
            IConsole console,
            IDataProcessor dataProcessor,
            string connectionString,
            string exchangeName)
        {
            _log = log;
            _console = console;
            _dataProcessor = dataProcessor;
            _connectionString = connectionString;
            _exchangeName = exchangeName;
        }

        public void Start()
        {
            var settings = RabbitMqSubscriptionSettings
                .CreateForSubscriber(_connectionString, _exchangeName, "rabbitmqtodisksaver")
                .MakeDurable();

            _subscriber = new RabbitMqSubscriber<Orderbook>(settings,
                    new ResilientErrorHandlingStrategy(_log, settings,
                        retryTimeout: TimeSpan.FromSeconds(10),
                        next: new DeadQueueErrorHandlingStrategy(_log, settings)))
                .SetMessageDeserializer(new JsonMessageDeserializer<Orderbook>())
                .Subscribe(ProcessMessageAsync)
                .CreateDefaultBinding()
                .SetLogger(_log)
                .SetConsole(_console)
                .Start();
        }

        private async Task ProcessMessageAsync(Orderbook item)
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
