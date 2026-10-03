using System;
using System.Text;
using RabbitMQ.Client;

namespace Publisher_cmd
{
    class RMQ
    {
        public ConnectionFactory connectionFactory;
        public IConnection connection;
        public IModel channel;

        // Inisialisasi parameter server RMQ[cite: 13]
        public void InitRMQConnection(string host = "localhost", int port = 5672, string user = "guest", string pass = "guest")
        {
            connectionFactory = new ConnectionFactory();
            connectionFactory.HostName = host;
            connectionFactory.UserName = user;
            connectionFactory.Password = pass;
        }

        // Membuka koneksi[cite: 13]
        public void CreateRMQConnection()
        {
            connection = connectionFactory.CreateConnection();
            Console.WriteLine("Koneksi " + (connection.IsOpen ? "Berhasil!" : "Gagal!"));
        }

        // Deklarasi Queue[cite: 13]
        public void CreateRMQChannel(string queue_name, string routingKey = "", string exchange_name = "")
        {
            if (connection.IsOpen)
            {
                channel = connection.CreateModel();
                Console.WriteLine("Channel " + (channel.IsOpen ? "Berhasil!" : "Gagal!"));
                if (channel.IsOpen)
                {
                    channel.QueueDeclare(queue: queue_name, durable: true, exclusive: false, autoDelete: false, arguments: null);
                    Console.WriteLine("Queue telah dideklarasikan..");
                }
            }
        }

        // Fungsi kirim pesan[cite: 13]
        public void SendMessage(string tujuan, string msg = "send")
        {
            byte[] responseBytes = Encoding.UTF8.GetBytes(msg);
            channel.BasicPublish(exchange: "", routingKey: tujuan, basicProperties: null, body: responseBytes);
            Console.WriteLine("Pesan: " + msg + " telah dikirim.");
        }

        // Fungsi putus koneksi[cite: 13]
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
            rmq.InitRMQConnection(); //

            Console.WriteLine("\nTekan tombol apapun untuk membuka koneksi ke RMQ.");
            Console.ReadKey();
            rmq.CreateRMQConnection(); //[cite: 14]

            Console.Write("\nMasukkan nama queue channel untuk mengirim pesan melalui RMQ.\n>>");
            string queue_name = Console.ReadLine();
            rmq.CreateRMQChannel(queue_name); //[cite: 14]

            Console.WriteLine("Masukkan pesan yang akan dikirim atau 'exit' to close.");
            Console.Write(">> tujuan: ");
            string tujuan = Console.ReadLine(); //[cite: 14]
            Console.Write(">> pesan: ");
            string inputMsg = Console.ReadLine(); //[cite: 14]

            while (inputMsg != "exit")
            {
                rmq.SendMessage(tujuan, inputMsg); //[cite: 14]
                Console.Write(">> tujuan: ");
                tujuan = Console.ReadLine();
                Console.Write(">> pesan: ");
                inputMsg = Console.ReadLine();
            }
            rmq.Disconnect(); //[cite: 14]
        }
    }
}