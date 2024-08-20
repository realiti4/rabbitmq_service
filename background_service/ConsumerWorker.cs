using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Threading.Channels;
using System.Text.Json;


namespace background_service
{
    class MessageData
    {
        public required string message { get; set; }
        public required string messageType { get; set; }
        public required int messageID { get; set; }
    }

    public class ConsumerWorker : BackgroundService, IDisposable
    {
        private readonly ILogger<ConsumerWorker> _logger;
        private IConnection _connection;
        private IModel _channel;

        public ConsumerWorker(ILogger<ConsumerWorker> logger)
        {
            _logger = logger;

            InitializeRabbitMQ();
        }

        private void InitializeRabbitMQ()
        {
            var factory = new ConnectionFactory
            {
                HostName = "127.0.0.1",
                //HostName = "host.docker.internal",    // For local docker development
                Port = 5672,
            };

            _connection = factory.CreateConnection();
            _channel = _connection.CreateModel();

            _channel.QueueDeclare(queue: "hello",
                     durable: false,
                     exclusive: false,
                     autoDelete: false,
                     arguments: null);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Consumer worker is running");

            //return Task.Run(() => Listen(stoppingToken));

            stoppingToken.ThrowIfCancellationRequested();

            var consumer = new EventingBasicConsumer(_channel);

            // Event with error handling
            consumer.Received += async (ch, ea) =>
            {
                try
                {
                    var content = Encoding.UTF8.GetString(ea.Body.ToArray());

                    MessageData? messageData = JsonSerializer.Deserialize<MessageData>(content);

                    if (messageData == null)
                    {
                        _logger.LogError("Message data is null");
                        _channel.BasicReject(ea.DeliveryTag, requeue: false);
                        return;
                    }

                    // Fake processing
                    await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);

                    _logger.LogInformation($"Received message: {content}");

                    _channel.BasicAck(ea.DeliveryTag, false);
                }
                catch (Exception ex)
                {
                    // TODO Handle number of retries here

                    _logger.LogError(ex, "Error occurred while processing message");
                    _channel.BasicReject(ea.DeliveryTag, requeue: true);
                }
            };

            //_channel.BasicQos(prefetchSize: 0, prefetchCount: 20, global: false);

            // Listen
            _channel.BasicConsume("hello", false, consumer);

            await Task.Delay(-1, stoppingToken);
        }

        public override void Dispose()
        {
            try
            {
                if (_channel != null && _channel.IsOpen)
                {
                    // Delete the queue
                    _channel.QueueDelete("hello");

                    // Close and dispose of the channel
                    _channel.Close();
                    _channel.Dispose();
                }

                if (_connection != null && _connection.IsOpen)
                {
                    // Close and dispose of the connection
                    _connection.Close();
                    _connection.Dispose();
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error occurred while disposing ConsumerWorker");
            }
            finally
            {
                base.Dispose();
            }
        }
    }
}
