using System;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Consumer_cmd
{
    class RMQ
    {
        public ConnectionFactory connectionFactory;
        public IConnection connection;
        public IModel channel;

        // Inisialisasi parameter RMQ[cite: 14]
        public void InitRMQConnection(string host = "localhost", int port = 5672, string user = "guest", string pass = "guest")
        {
            connectionFactory = new ConnectionFactory();
            connectionFactory.HostName = host;
            connectionFactory.UserName = user;
            connectionFactory.Password = pass;
        }

        // Buka koneksi[cite: 14]
        public void CreateRMQConnection()
        {
            connection = connectionFactory.CreateConnection();
            Console.WriteLine("Koneksi " + (connection.IsOpen ? "Berhasil!" : "Gagal!"));
        }

        // Menunggu pesan masuk[cite: 14]
        public void WaitingMessage(string queue_name)
        {
            channel = connection.CreateModel();
            channel.QueueDeclare(queue: queue_name, durable: true, exclusive: false, autoDelete: false, arguments: null); //[cite: 15]

            var consumer = new EventingBasicConsumer(channel); //[cite: 15]
            consumer.Received += (model, ea) =>
            {
                var body = ea.Body.ToArray(); // Penyesuaian API baru
                var message = Encoding.UTF8.GetString(body); //[cite: 15]
                Console.WriteLine(" [x] Pesan diterima: {0}", message); //[cite: 15]
            };

            channel.BasicConsume(queue: queue_name, autoAck: true, consumer: consumer); //[cite: 15]
            Console.WriteLine(" Tekan [enter] untuk memutus koneksi."); //[cite: 15]
            Console.ReadLine(); //[cite: 15]
            Disconnect(); //[cite: 15]
        }

        // Putus koneksi[cite: 15]
        public void Disconnect()
        {
            channel.Close();
            Console.WriteLine("Channel ditutup!");
            if (connection.IsOpen)
            {
                connection.Close();
                Console.WriteLine("Koneksi diputus!");
            }
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            RMQ rmq = new RMQ();
            Console.WriteLine("Tekan tombol apapun untuk inisialisasi RMQ parameters.");
            Console.ReadKey();
            rmq.InitRMQConnection(); //[cite: 15]

            Console.WriteLine("\nTekan tombol apapun untuk membuka koneksi ke RMQ.");
            Console.ReadKey();
            rmq.CreateRMQConnection(); //[cite: 15]

            Console.Write("\nMasukkan nama queue channel untuk menerima pesan melalui RMQ.\n>>");
            string queue_name = Console.ReadLine(); //[cite: 15]
            
            Console.WriteLine("Menunggu pesan masuk...");
            rmq.WaitingMessage(queue_name); //[cite: 15]
        }
    }
}