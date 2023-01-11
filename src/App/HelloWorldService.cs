using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using HNSC.CL.II.HisData.RealtimeOnlines;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Newtonsoft.Json;
using Volo.Abp.Caching;
using Volo.Abp.DependencyInjection;
using Volo.Abp.ObjectMapping;

namespace App;

public class HelloWorldService : ITransientDependency
{
    public ILogger<HelloWorldService> Logger { get; set; }
    private readonly IDistributedCache<HNSC.CL.II.HisData.RealtimeOnlines.RealtimeOnline, string> _realtimeOnlineCache;
    private readonly IObjectMapper _objectMapper;

    public HelloWorldService(IDistributedCache<HNSC.CL.II.HisData.RealtimeOnlines.RealtimeOnline, string> realtimeOnlineCache, IObjectMapper objectMapper)
    {
        Logger = NullLogger<HelloWorldService>.Instance;
        _realtimeOnlineCache = realtimeOnlineCache;
        _objectMapper = objectMapper;
    }

    public async Task SayHelloAsync()
    {
        Logger.LogInformation("Hello World!");

        // 读取参数表
        var bigMappingsStr = await File.ReadAllTextAsync("bigMappings.json");
        BasicRedisDto basicRedisDto = JsonConvert.DeserializeObject<BasicRedisDto>(bigMappingsStr);

        // 构成实时数据表
        List<RealtimeOnlineDto> realtimeOnlineDtos = basicRedisDto.items.Select(item => new RealtimeOnlineDto
        {
            EnterpriseCode = item.EnterpriseCode,
            SpecialtyCategory = SpecialtyCategory.S30,
            DeviceNumber = item.DeviceNumber,
            ParamDictNumber = item.DictNumber,
            RecordDate = DateTime.Now,
            RecordValue = "123.45"
        }).ToList();

        // 构成键值对
        var realtimeOnlineKVs = realtimeOnlineDtos.Select(realtimeOnlineDto =>
            new KeyValuePair<string, RealtimeOnline>(
                $"{realtimeOnlineDto.EnterpriseCode},{realtimeOnlineDto.DeviceNumber},{realtimeOnlineDto.ParamDictNumber},{realtimeOnlineDto.SpecialtyCategory}",
                _objectMapper.Map<RealtimeOnlineDto, RealtimeOnline>(realtimeOnlineDto)
            )).ToArray();

        var keys = realtimeOnlineDtos.Select(realtimeOnlineDto => $"{realtimeOnlineDto.EnterpriseCode},{realtimeOnlineDto.DeviceNumber},{realtimeOnlineDto.ParamDictNumber},{realtimeOnlineDto.SpecialtyCategory}")
            .Distinct().ToList();

        // 清理环境
        await _realtimeOnlineCache.RemoveManyAsync(keys);

        var first = realtimeOnlineKVs.FirstOrDefault();

        // SetAsync
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            await _realtimeOnlineCache.SetAsync(first.Key, first.Value);
            sw.Stop();
            Logger.LogInformation($"SetAsync 执行时间：{sw.ElapsedMilliseconds} ms");
        }

        // GetAsync
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            var value = await _realtimeOnlineCache.GetAsync(first.Key);
            sw.Stop();
            Logger.LogInformation($"GetAsync 执行时间：{sw.ElapsedMilliseconds} ms，值为：{JsonConvert.SerializeObject(value)}");
        }

        // SetManyAsync
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            await _realtimeOnlineCache.SetManyAsync(realtimeOnlineKVs);
            sw.Stop();
            Logger.LogInformation($"SetManyAsync 执行时间：{sw.ElapsedMilliseconds} ms");
        }

        // GetManyAsync
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            KeyValuePair<string, RealtimeOnline>[] kvs = await _realtimeOnlineCache.GetManyAsync(keys);
            sw.Stop();
            Logger.LogInformation($"GetManyAsync 执行时间：{sw.ElapsedMilliseconds} ms，{kvs.Count()} 条");
        }

        // GetOrAddManyAsync
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            KeyValuePair<string, RealtimeOnline>[] kvs = await _realtimeOnlineCache.GetOrAddManyAsync(keys, (missingKeys) =>
            {
                Logger.LogInformation($"missingKeys: {missingKeys.Count()} 个");
                return null;
            }, () => new DistributedCacheEntryOptions
            {
                AbsoluteExpiration = DateTimeOffset.Now.AddMinutes(5)
            });
            sw.Stop();
            Logger.LogInformation($"GetOrAddManyAsync 执行时间：{sw.ElapsedMilliseconds} ms，{kvs.Count()} 条");
        }

        // 清理环境
        await _realtimeOnlineCache.RemoveManyAsync(keys);
    }
}
