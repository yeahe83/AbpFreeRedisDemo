using System.Collections.Generic;

namespace App;

public class BasicRedisDto
{
    public List<WorkerBasicInfoDto> items { get; set; }
}

public class WorkerBasicInfoDto
{
    /// <summary>
    /// 企业编号
    /// </summary>
    public string EnterpriseCode { get; set; }

    /// <summary>
    /// 设备编号
    /// </summary>
    public string DeviceNumber { get; set; }

    /// <summary>
    /// 参数字典编号
    /// </summary>
    public string DictNumber { get; set; }

    /// <summary>
    /// 公式
    /// </summary>
    public string Formula { get; set; }

    /// <summary>
    /// 报警描述
    /// </summary>
    public string AlarmDescription { get; set; }

    /// <summary>
    /// 忽略报警天数
    /// </summary>
    public double SkipCheckDays { get; set; }

    /// <summary>
    /// 数据源编码
    /// </summary>
    public string SourceNumber { get; set; }

    /// <summary>
    /// 参数类型
    /// </summary>
    public ParameterType ParameterType { get; set; }

    /// <summary>
    /// 专业
    /// </summary>
    public SpecialtyCategory SpecialtyCategory { get; set; }
}