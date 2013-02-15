using System.Net;
using System.Transactions;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Rhino.Queues.Model;

namespace Rhino.Queues.Tests
{
    [TestFixture]
    public class When_try_to_connect_to_queue
    {
        [Test]
        public void Should_connect_to_the_queue()
        {
            var receiver = new QueueManager(new IPEndPoint(IPAddress.Loopback, 4545), "receiver.esent");

            using (var tx = new TransactionScope())
            {
                receiver.CreateQueues("uno");
                receiver.Start();

                tx.Complete();   
            }

            Sender(1);
        }

        public void Sender(int count)
        {
            using (var sender = new QueueManager(new IPEndPoint(IPAddress.Loopback, 4546), "sender.esent"))
            {
                sender.Start();
                using (var tx = new TransactionScope())
                {
                    sender.Send(new Uri("rhino.queues://localhost:4545/uno"),
                                new MessagePayload
                                {
                                    Data = Encoding.ASCII.GetBytes("Message " + count)
                                }
                        );
                    tx.Complete();
                }
                sender.WaitForAllMessagesToBeSent();
            }
        }
    }
}
