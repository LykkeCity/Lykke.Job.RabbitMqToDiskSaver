using Autofac;
using Common.Log;
using Lykke.Job.RabbitMqToDiskSaver.Core.Services;
using Lykke.Job.RabbitMqToDiskSaver.Settings;
using Lykke.Job.RabbitMqToDiskSaver.Services;
using Lykke.Job.RabbitMqToDiskSaver.RabbitSubscribers;

namespace Lykke.Job.RabbitMqToDiskSaver.Modules
{
    public class JobModule : Module
    {
        private readonly RabbitMqToDiskSaverSettings _settings;
        private readonly ILog _log;
        private readonly IConsole _console;

        public JobModule(RabbitMqToDiskSaverSettings settings, ILog log, IConsole console)
        {
            _settings = settings;
            _log = log;
            _console = console;
        }

        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterInstance(_log)
                .As<ILog>()
                .SingleInstance();

            builder.RegisterInstance(_console)
                .As<IConsole>()
                .SingleInstance();

            builder.RegisterType<HealthService>()
                .As<IHealthService>()
                .SingleInstance();

            builder.RegisterType<StartupManager>()
                .As<IStartupManager>();

            builder.RegisterType<ShutdownManager>()
                .As<IShutdownManager>();

            builder.RegisterType<DiskWorker>()
                .As<IDiskWorker>()
                .As<IStartable>()
                .AutoActivate()
                .SingleInstance();

            builder.RegisterType<DataProcessor>()
                .As<IDataProcessor>()
                .As<IStartable>()
                .AutoActivate()
                .SingleInstance()
                .WithParameter(TypedParameter.From(_settings.DiskPath))
                .WithParameter("diskPath", _settings.DiskPath)
                .WithParameter("warningSizeInGigabytes", _settings.WarningSizeInGigabytes)
                .WithParameter("maxSizeInGigabytes", _settings.MaxSizeInGigabytes);

            builder.RegisterType<RabbitMessageSubscriber>()
                .As<IStartable>()
                .AutoActivate()
                .SingleInstance()
                .WithParameter("connectionString", _settings.Rabbit.ConnectionString)
                .WithParameter("exchangeName", _settings.Rabbit.ExchangeName);
        }
    }
}
