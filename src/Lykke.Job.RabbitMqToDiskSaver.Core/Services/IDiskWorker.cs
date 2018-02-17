namespace Lykke.Job.RabbitMqToDiskSaver.Core.Services
{
    public interface IDiskWorker
    {
        void AddDataItem(byte[] data);
    }
}
