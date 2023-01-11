using System;

namespace App;

public class RealtimeOnlineDto
{
    public string EnterpriseCode { get; set; }
    public SpecialtyCategory SpecialtyCategory { get; set; }
    public string DeviceNumber { get; set; }
    public string ParamDictNumber { get; set; }
    public DateTime RecordDate { get; set; }
    public string RecordValue { get; set; }
}