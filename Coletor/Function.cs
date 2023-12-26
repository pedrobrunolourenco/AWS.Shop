using Amazon.Lambda.Core;
using Amazon.Lambda.DynamoDBEvents;
using Amazon.DynamoDBv2.Model;
using Model;
using Amazon.DynamoDBv2;
using Compartilhado;
using Amazon;


[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace Coletor;

public class Function
{
    public async Task FunctionHandler(DynamoDBEvent dynamoEvent, ILambdaContext context)
    {
        foreach (var record in dynamoEvent.Records)
        {
            if (record.EventName == "INSERT")
            {
                var pedido = record.Dynamodb.NewImage.ToObject<Pedido>();
                pedido.Status = StatusDoPedido.Coletado;
                try
                {
                    await ProcessarValorDoPedido(pedido, context);
                    context.Logger.LogLine($"Sucesso => {pedido.Id}");
                    await AmazonUtil.EnviarParaFila(EnumFilasSQS.pedido, pedido);
                }
                catch (Exception ex)
                {
                    context.Logger.LogLine($"Erro => {ex.Message}");
                    pedido.JustificativaDeCancelamento = ex.Message;
                    pedido.Status = StatusDoPedido.Falha;
                    pedido.Cancelado = true;
                    await AmazonUtil.EnviarParaFila(EnumFilasSNS.falha, pedido);
                }
                await pedido.SalvarAsync();
            }
        }
    }
    private async Task ProcessarValorDoPedido(Pedido pedido, ILambdaContext context)
    {
        foreach (var produto in pedido.Produtos)
        {
            Produto? produtoDoEstoque = await ObterProdutoDoDynamoDBAsync(produto.Id, context);

            if (produtoDoEstoque == null) throw new InvalidOperationException($"Produto não encontrado na tabela estoque. {produto.Id}");
            produto.Valor = produtoDoEstoque.Valor;
            produto.Nome = produtoDoEstoque.Nome;
        }

        var valorTotal = pedido.Produtos.Sum(x => x.Valor * x.Quantidade);
        if (pedido.ValorTotal != 0 && pedido.ValorTotal != valorTotal)
            throw new InvalidOperationException($"O valor esperado do pedido é de R$ {pedido.ValorTotal} e o valor verdadeiro é R$ {valorTotal}");

        pedido.ValorTotal = valorTotal;
    }

    private async Task<Produto?> ObterProdutoDoDynamoDBAsync(string id, ILambdaContext context)
    {
        var client = new AmazonDynamoDBClient(RegionEndpoint.SAEast1);
        var request = new QueryRequest
        {
            TableName = "Estoque",
            KeyConditionExpression = "Id = :v_id",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { { ":v_id", new AttributeValue { S = id } } }
        };
        var response = await client.QueryAsync(request);
        var item = response.Items.FirstOrDefault();
        if (item == null) return null;
        return item.ToObject<Produto>();
    }



}