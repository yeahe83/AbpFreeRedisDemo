using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Volo.Abp;
using Volo.Abp.Autofac;
using Volo.Abp.Modularity;
using Volo.Abp.Caching;
using SharpAbp.Abp.FreeRedis;
using Volo.Abp.ObjectMapping;
using Volo.Abp.AutoMapper;

namespace App;

[DependsOn(typeof(AbpAutofacModule))]
[DependsOn(typeof(AbpCachingModule))]
[DependsOn(typeof(AbpObjectMappingModule))]
[DependsOn(typeof(AbpAutoMapperModule))]
[DependsOn(typeof(AbpFreeRedisModule))]
public class AppModule : AbpModule
{
    public override Task OnApplicationInitializationAsync(ApplicationInitializationContext context)
    {
        var logger = context.ServiceProvider.GetRequiredService<ILogger<AppModule>>();
        var configuration = context.ServiceProvider.GetRequiredService<IConfiguration>();
        logger.LogInformation($"MySettingName => {configuration["MySettingName"]}");

        var hostEnvironment = context.ServiceProvider.GetRequiredService<IHostEnvironment>();
        logger.LogInformation($"EnvironmentName => {hostEnvironment.EnvironmentName}");

        return Task.CompletedTask;
    }

    public override void ConfigureServices(ServiceConfigurationContext context)
    {
        Configure<AbpDistributedCacheOptions>(options =>
        {
            options.KeyPrefix = "162.HNSC.VOC.II";
        });

        // https://github.com/cocosip/sharp-abp/blob/master/framework/test/SharpAbp.Abp.FreeRedis.Tests/SharpAbp/Abp/FreeRedis/AbpFreeRedisTestModule.cs
        Configure<AbpFreeRedisOptions>(options =>
        {
            options.Clients.ConfigureDefault(client =>
            {
                client.Mode = RedisMode.Single;
                // client.ConnectionString = "192.168.1.150:6379";
                client.ConnectionString = "192.168.1.165:30379";
                client.ReadOnly = false;
            });
        });

        Configure<AbpAutoMapperOptions>(options =>
        {
            //Add all mappings defined in the assembly of the MyModule class
            options.AddMaps<AppModule>();
        });

        base.ConfigureServices(context);
    }
}
