using System.ComponentModel;

namespace RabbitSharp.Enums
{
    public enum ExchangeTypeEnum
    {
        [Description("direct")]
        Direct,

        [Description("fanout")]
        Fanout,

        [Description("topic")]
        Topic
    }
}