using System.Threading.Tasks;
using Lykke.Job.RabbitMqToDiskSaver.Core.Domain.Models;

namespace Lykke.Job.RabbitMqToDiskSaver.Core.Services
{
    public interface IDataToDiskSaver
    {
        Task SaveDataItemAsync(Orderbook item);
    }
}
