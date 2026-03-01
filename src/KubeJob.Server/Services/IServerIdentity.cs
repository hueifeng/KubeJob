using System;

namespace KubeJob.Server.Services
{
    public interface IServerIdentity
    {
        string ServerId { get; }
    }

    public class DefaultServerIdentity : IServerIdentity
    {
        public string ServerId { get; }

        public DefaultServerIdentity()
        {
            ServerId = Guid.NewGuid().ToString("N");
        }
    }
}
