using System.Threading.Tasks;

namespace Lykke.Job.RabbitMqToDiskSaver.Core.Services
{
    public interface IDataToDiskSaver
    {
        Task SaveDataItemAsync(byte[] data);
    }
}
