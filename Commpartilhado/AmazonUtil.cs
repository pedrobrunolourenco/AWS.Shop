using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2.DocumentModel;
using Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using Newtonsoft.Json;
using static System.Net.WebRequestMethods;
using Amazon.Lambda.Core;

namespace Compartilhado
{
    public static class AmazonUtil
    {

        public static async Task SalvarAsync( this Pedido pedido)
        {
            var cliente = new AmazonDynamoDBClient(RegionEndpoint.SAEast1);
            var context = new DynamoDBContext(cliente);
            await context.SaveAsync(pedido);
        }

        public static T ToObject<T>(this Dictionary<string, AttributeValue> dictionary)
        {
            var client = new AmazonDynamoDBClient(RegionEndpoint.SAEast1);
            var context = new DynamoDBContext(client);

            var doc = Document.FromAttributeMap(dictionary);
            return context.FromDocument<T>(doc);
        }

        public static async Task EnviarParaFila(EnumFilasSQS fila, Pedido pedido)
        {
            var client = new AmazonSQSClient(RegionEndpoint.SAEast1);

            var json = JsonConvert.SerializeObject(pedido);

            var request = new SendMessageRequest
            {
                QueueUrl = $"https://sqs.sa-east-1.amazonaws.com/390428001522/{fila}",
                MessageBody = json
            };

            await client.SendMessageAsync(request);
        }

        public static async Task EnviarParaFila(EnumFilasSNS fila, Pedido pedido, ILambdaContext context)
        {
            context.Logger.LogLine("ENTREI AQUI 02");

            // Implementar
            var client = new AmazonSQSClient(RegionEndpoint.SAEast1);

            var json = JsonConvert.SerializeObject(pedido);

            var request = new SendMessageRequest
            {
                QueueUrl = $"https://sqs.sa-east-1.amazonaws.com/390428001522/{fila}",
                MessageBody = json
            };

            await client.SendMessageAsync(request);
            context.Logger.LogLine("ENTREI AQUI 03");

        }



    }
}
