using System.ComponentModel;

namespace RabbitSharp.Enums
{
    public enum OverFlowStrategyEnum
    {
        [Description("reject-publish")]
        RejectPublish,

        [Description("drop-head")]
        DropHead,
    }
}