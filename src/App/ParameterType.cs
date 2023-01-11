namespace App;

public enum ParameterType
{
    /// <summary>
    /// 未定义
    /// </summary>
    Undefined = 0,

    /// <summary>
    /// 在线参数
    /// </summary>
    Online = 1,

    /// <summary>
    /// 离线参数
    /// </summary>
    Offline = 2,

    /// <summary>
    /// 计算参数
    /// </summary>
    Compute = 3,

    /// <summary>
    /// 报警参数
    /// </summary>
    Alarm = 4
}