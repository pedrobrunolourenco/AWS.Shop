using Amazon.DynamoDBv2.DataModel;
using Newtonsoft.Json.Converters;
using System.Text.Json.Serialization;

namespace Model
{


    public enum StatusDoPedido
    {
        Coletado,
        Reservado,
        Pago,
        Faturado,
        Falha
    }


    [DynamoDBTable("Pedidos")]
    public class Pedido
    {
        public string? Id { get; set; }

        public decimal ValorTotal { get; set; }

        public DateTime DataDeCriacao { get; set; }

        public List<Produto>? Produtos { get; set; }

        public Cliente? Cliente { get; set; }

        public Pagamento? Pagamento { get; set; }
       
        public string? JustificativaDeCancelamento { get; set; }

        // [JsonConverter(typeof(StringEnumConverter))]
        public StatusDoPedido Status { get; set; }

        public bool Cancelado { get; set; }

    }
}
