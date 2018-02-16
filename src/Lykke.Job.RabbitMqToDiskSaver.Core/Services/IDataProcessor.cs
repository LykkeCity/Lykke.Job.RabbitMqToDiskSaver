using Lykke.Job.RabbitMqToDiskSaver.Core.Domain.Models;

namespace Lykke.Job.RabbitMqToDiskSaver.Core.Services
{
    public interface IDataProcessor
    {
        void Process(Orderbook item);
    }
}
