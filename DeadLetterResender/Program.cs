using Microsoft.ServiceBus.Messaging;
using System;
using System.Collections.Specialized;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;

namespace Softwillians.ServiceBus.DeadLetterResender
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            NameValueCollection _appSettings = ConfigurationManager.AppSettings;

            string _connectionString = _appSettings.GetOrThrow("QueueConnectionString");
            string _queueName = _appSettings.GetOrThrow("QueueName");
            string _messageLabel = _appSettings.GetOrThrow("MessageLabel");

            await ResendDeadLetters(_connectionString, _queueName, _messageLabel);
        }
        public static async Task ResendDeadLetters(string servicebusConnectionString, string queueName, string messageLabel)
        {
            string _logFilePath = Path.Combine(Directory.GetCurrentDirectory(), $"deadletter-{DateTime.Now.Ticks}-log.json");
            string _errorLogFilePath = Path.Combine(Directory.GetCurrentDirectory(), $"deadletter-errors-{DateTime.Now.Ticks}-log.txt");

            StreamWriter _logFile = File.CreateText(_logFilePath);
            StreamWriter _logErrorFile = File.CreateText(_errorLogFilePath);

            var messageFactory = MessagingFactory.CreateFromConnectionString(servicebusConnectionString);

            MessageReceiver deadletterReceiver = await messageFactory.CreateMessageReceiverAsync(QueueClient.FormatDeadLetterPath(queueName),
                ReceiveMode.PeekLock);

            MessageSender sender = await messageFactory.CreateMessageSenderAsync(queueName);

            BrokeredMessage _deadLetterMessage;

            do
            {
                _deadLetterMessage = await deadletterReceiver.ReceiveAsync(TimeSpan.Zero);

                if (_deadLetterMessage == null)
                    break;
                else
                {
                    Stream _body = _deadLetterMessage.GetBody<Stream>();
                    StreamReader _streamReader = new StreamReader(_body);
                    string _messageContent = _streamReader.ReadToEnd();

                    var _message = new BrokeredMessage(_body)
                    {
                        ContentType = _deadLetterMessage.ContentType,
                        CorrelationId = _deadLetterMessage.CorrelationId,
                        Label = messageLabel
                    };

                    Debugger.Log(0, "INFO", $"Mensagem {_deadLetterMessage.SequenceNumber} {_deadLetterMessage.MessageId} recebida da dead letter\n");

                    try
                    {
                        await sender.SendAsync(_message);
                        await _deadLetterMessage.CompleteAsync();

                        _logFile.Write(_messageContent.ToString());
                        _logFile.WriteLine(",");
                    }
                    catch (Exception e)
                    {
                        Debugger.Log(1, "WARN", e.Message);
                        Debugger.Log(1, "WANR", "Erro ao processar mensagem\n");
                        _logErrorFile.WriteLine($"{e.Message} {_message.MessageId},");
                    }
                }

            } while (_deadLetterMessage != null);

            _logFile.Close();
            _logErrorFile.Close();
        }

    }
}
