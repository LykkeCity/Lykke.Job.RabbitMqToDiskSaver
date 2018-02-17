namespace Lykke.Job.RabbitMqToDiskSaver.Core.Services
{
    public interface IDataProcessor
    {
        void Process(byte[] data);
    }
}
