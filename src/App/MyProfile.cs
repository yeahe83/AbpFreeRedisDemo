using AutoMapper;
using HNSC.CL.II.HisData.RealtimeOnlines;

namespace App;

public class MyProfile : Profile
{
    public MyProfile()
    {
        CreateMap<RealtimeOnlineDto, RealtimeOnline>();
    }
}