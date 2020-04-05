using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
using Amqp;
using Amqp.Framing;
using log4net;
using Timer = System.Timers.Timer;

namespace AmqpSenderService
{
    public class Sender : IDisposable
    {
        private readonly int Interval = 30;

        // AMQP connection selection
        List<Address> addresses;
        int aIndex = 0;

        // Protocol objects
        Connection connection;
        Session session;
        SenderLink sender;

        // Time in mS to wait for a connection to connect and become sendable
        // before failing over to the next host.
        const Int32 WAIT_TIME = 10 * 1000;

        private readonly ManualResetEvent _connected = new ManualResetEvent(false);

        // Application mission state
        private readonly ILog _log;
        private readonly Timer _timer;
        ulong messageCounter = 0;

        /// <summary>
        /// Application constructor
        /// </summary>
        /// <param name="_addresses">Address objects that define the host, port, and target for messages.</param>
        /// <param name="_nToSend">Message count.</param>
        public Sender(ILog log)
        {
            #region Adresses

            string addrs = "amqp://guest:guest@localhost:5672";

            addresses = new List<Address>();
            foreach (var adr in addrs.Split(',').ToList())
            {
                addresses.Add(new Address(adr));
            }

            #endregion

            _log = log;

            if (int.TryParse(ConfigurationManager.AppSettings["Interval"], out var intervalFromConfig))
            {
                Interval = intervalFromConfig;
            }

            _timer = new Timer(TimeSpan.FromSeconds(Interval).TotalMilliseconds)
            {
                AutoReset = true
            };
            _timer.Elapsed += TimerOnElapsed;
        }

        private void TimerOnElapsed(object o, ElapsedEventArgs e)
        {
            try
            {
                _log.InfoFormat("Sending message {0}", messageCounter);

                Message message = new Message("message " + messageCounter);
                message.Properties = new Properties();
                message.Properties.SetMessageId((object)messageCounter);
                sender.Send(message);
                messageCounter += 1;

                _log.InfoFormat("Sent    message {0}", messageCounter - 1);
            }
            catch (Exception ex)
            {
                _log.ErrorFormat("Exception sending message {0}: {1}", messageCounter, ex.Message);
                Reconnect();
            }
        }

        /// <summary>
        /// Connection closed event handler
        /// 
        /// This function provides information only. Calling Reconnect is redundant with
        /// calls from the Run loop.
        /// </summary>
        /// <param name="_">Connection that closed. There is only one connection so this is ignored.</param>
        /// <param name="error">Error object associated with connection close.</param>
        void ConnectionClosed(IAmqpObject _, Error error)
        {
            if (error == null)
                _log.WarnFormat("Connection closed with no error");
            else
                _log.ErrorFormat("Connection closed with error: {0}", error.ToString());
        }

        private void SessionOnClosed(IAmqpObject amqpObject, Error error)
        {
            if (error == null)
                _log.WarnFormat("Session closed with no error");
            else
                _log.ErrorFormat("Session closed with error: {0}", error);
        }

        private void SenderOnClosed(IAmqpObject amqpObject, Error error)
        {
            if (error == null)
                _log.WarnFormat("Sender closed with no error");
            else
                _log.ErrorFormat("Sender closed with error: {0}", error);
        }

        /// <summary>
        /// Select the next host in the Address list and start it
        /// </summary>
        void Reconnect()
        {
            _log.InfoFormat("Entering Reconnect()");

            _connected.Reset();

            if (!_connected.WaitOne(WAIT_TIME))
            {
                OpenConnection();
            }
        }


        /// <summary>
        /// Start the current host in the address list
        /// </summary>
        async void OpenConnection()
        {
            try
            {
                _log.InfoFormat("Attempting connection to  {0}:{1}", addresses[aIndex].Host, addresses[aIndex].Port);

                connection = await Connection.Factory.CreateAsync(addresses[aIndex], null, OnOpened);
                connection.Closed += ConnectionClosed;

                _log.InfoFormat("Success: connecting to {0}:{1}", addresses[aIndex].Host, addresses[aIndex].Port);
            }
            catch (Exception e)
            {
                _log.ErrorFormat("Failure: exception connecting to '{0}:{1}': {2}", addresses[aIndex].Host, addresses[aIndex].Port, e.Message);
            }
        }

        /// <summary>
        /// AMQP connection has opened. This callback may be called before
        /// ConnectAsync returns a value to the _connection_ variable.
        /// </summary>
        /// <param name="conn">Which connection. </param>
        /// <param name="__">Peer AMQP Open (ignored).</param>
        void OnOpened(IConnection conn, Open __)
        {
            _log.InfoFormat("Event: OnOpened");

            connection = (Connection)conn;
            _connected.Set();

            session = new Session(connection, new Begin() { }, OnBegin);
            session.Closed += SessionOnClosed;
        }


        /// <summary>
        /// AMQP session has opened
        /// </summary>
        /// <param name="_">Which session (ignored).</param>
        /// <param name="__">Peer AMQP Begin (ignored).</param>
        void OnBegin(ISession _, Begin __)
        {
            _log.InfoFormat("Event: OnBegin");

            string targetName = addresses[aIndex].Path.Substring(1); // no leading '/'
            Target target = new Target() { Address = targetName };
            sender = new SenderLink(session, "senderLink", target, OnAttached);
            sender.Closed += SenderOnClosed;
        }

        /// <summary>
        /// AMQP Link has attached. Signal that protocol stack is ready to send.
        /// </summary>
        /// <param name="_">Which link (ignored).</param>
        /// <param name="__">Peer AMQP Attach (ignored).</param>
        void OnAttached(ILink _, Attach __)
        {
            _log.InfoFormat("Event: OnAttached");
        }

        /// <summary>
        /// Application mission code.
        /// Send N messages while automatically reconnecting to broker/peer as necessary.
        /// </summary>
        public void Start()
        {
            OpenConnection();
            _timer.Start();
        }

        public void Dispose()
        {
            _timer.Stop();

            var closeSender = sender.CloseAsync();

            var closeSession = session.CloseAsync();

            var closeConnection = connection.CloseAsync();

            Task.WaitAll(closeSender, closeSession, closeConnection);
        }
    }
}