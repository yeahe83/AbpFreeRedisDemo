using System;

namespace HNSC.CL.II.HisData.RealtimeOnlines;

public class RealtimeOnline
{
    public Guid Id { get; set; }
    public string EnterpriseCode { get; set; }
    public int SpecialtyCategory { get; set; }
    public string DeviceNumber { get; set; }
    public string ParamDictNumber { get; set; }
    public DateTime RecordDate { get; set; }
    public string RecordValue { get; set; }
}