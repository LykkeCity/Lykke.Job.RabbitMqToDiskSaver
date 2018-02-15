using System.Threading.Tasks;

namespace Lykke.Job.RabbitMqToDiskSaver.Core.Services
{
    public interface IShutdownManager
    {
        Task StopAsync();
    }
}