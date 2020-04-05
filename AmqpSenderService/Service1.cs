using log4net;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace AmqpSenderService
{
    public partial class Service1 : ServiceBase
    {
        private readonly Sender _sender;
        private readonly ILog _log;

        public Service1(Sender sender, ILog log)
        {
            _sender = sender;
            _log = log;

            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            _log.Info("In OnStart.");

            _sender.Start();
        }

        protected override void OnStop()
        {
            _log.Info("In OnStop.");

            _sender.Dispose();
        }

        internal void Start()
        {
            OnStart(null);

            Console.WriteLine("Service started. Press any key to shutdown the service.");
            Console.ReadLine();
        }
    }
}
