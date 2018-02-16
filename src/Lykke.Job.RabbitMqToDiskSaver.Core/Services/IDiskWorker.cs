namespace Lykke.Job.RabbitMqToDiskSaver.Core.Services
{
    public interface IDiskWorker
    {
        void AddDataItem(string text, string directoryPath);
    }
}
