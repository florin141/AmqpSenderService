using Autofac;
using System.Collections.Generic;
using System.Linq;
using Amqp;

[assembly: log4net.Config.XmlConfigurator]

namespace AmqpSenderService
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main()
        {
            #region Autofac

            var builder = new ContainerBuilder();

            builder.RegisterModule(new LoggingModule());
            builder.RegisterType<Sender>();
            builder.RegisterType<Service1>();

            var container = builder.Build();

            #endregion Autofac

            var service = container.Resolve<Service1>();
#if DEBUG
            service.Start();
            service.Stop();
#else
            System.ServiceProcess.ServiceBase.Run(service);
#endif
        }
    }
}
