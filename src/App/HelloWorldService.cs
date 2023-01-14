using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FreeRedis;
using HNSC.CL.II.HisData.RealtimeOnlines;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using SharpAbp.Abp.FreeRedis;
using Volo.Abp.Caching;
using Volo.Abp.DependencyInjection;
using Volo.Abp.ObjectMapping;

namespace App;

public class HelloWorldService : ITransientDependency
{
    public ILogger<HelloWorldService> Logger { get; set; }
    private readonly IDistributedCache<HNSC.CL.II.HisData.RealtimeOnlines.RealtimeOnline, string> _realtimeOnlineCache;
    private readonly IObjectMapper _objectMapper;
    private readonly IRedisClientFactory _clientFactory;
    private readonly RedisClient _client;
    private readonly AbpDistributedCacheOptions _distributedCacheOption;

    public HelloWorldService(IDistributedCache<HNSC.CL.II.HisData.RealtimeOnlines.RealtimeOnline, string> realtimeOnlineCache, IObjectMapper objectMapper, IRedisClientFactory clientFactory, IOptions<AbpDistributedCacheOptions> distributedCacheOption)
    {
        Logger = NullLogger<HelloWorldService>.Instance;
        _realtimeOnlineCache = realtimeOnlineCache;
        _objectMapper = objectMapper;
        _clientFactory = clientFactory;
        _client = _clientFactory.Get();
        _client.Serialize = obj => JsonConvert.SerializeObject(obj);
        _client.Deserialize = (json, type) => JsonConvert.DeserializeObject(json, type);
        _distributedCacheOption = distributedCacheOption.Value;
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
        // await _realtimeOnlineCache.RemoveManyAsync(keys);
        FreeRemoveMany<RealtimeOnline>(_client, _distributedCacheOption.KeyPrefix, keys);

        var first = realtimeOnlineKVs.FirstOrDefault();

        // SetAsync
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            // await _realtimeOnlineCache.SetAsync(first.Key, first.Value);
            FreeSet(_client, _distributedCacheOption.KeyPrefix, first.Key, first.Value, new DistributedCacheEntryOptions
            {
                AbsoluteExpiration = DateTimeOffset.Now.AddMinutes(5)
            });
            sw.Stop();
            Logger.LogInformation($"SetAsync 执行时间：{sw.ElapsedMilliseconds} ms");
        }

        // GetAsync
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            // var value = await _realtimeOnlineCache.GetAsync(first.Key);
            var value = FreeGet<RealtimeOnline>(_client, _distributedCacheOption.KeyPrefix, first.Key);
            sw.Stop();
            Logger.LogInformation($"GetAsync 执行时间：{sw.ElapsedMilliseconds} ms，值为：{JsonConvert.SerializeObject(value)}");
        }

        // SetManyAsync
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            // await _realtimeOnlineCache.SetManyAsync(realtimeOnlineKVs);
            FreeSetMany<RealtimeOnline>(_client, _distributedCacheOption.KeyPrefix, realtimeOnlineKVs, new DistributedCacheEntryOptions
            {
                AbsoluteExpiration = DateTimeOffset.Now.AddMinutes(5)
            });
            sw.Stop();
            Logger.LogInformation($"SetManyAsync 执行时间：{sw.ElapsedMilliseconds} ms");
        }

        // GetManyAsync
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            // KeyValuePair<string, RealtimeOnline>[] kvs = await _realtimeOnlineCache.GetManyAsync(keys);
            KeyValuePair<string, RealtimeOnline>[] kvs = FreeGetMany<RealtimeOnline>(_client, _distributedCacheOption.KeyPrefix, keys);
            sw.Stop();
            Logger.LogInformation($"GetManyAsync 执行时间：{sw.ElapsedMilliseconds} ms，{kvs.Count()} 条");
        }

        // GetOrAddManyAsync
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            // KeyValuePair<string, RealtimeOnline>[] kvs = await _realtimeOnlineCache.GetOrAddManyAsync(keys, (missingKeys) =>
            // {
            //     Logger.LogInformation($"missingKeys: {missingKeys.Count()} 个");
            //     return null;
            // }, () => new DistributedCacheEntryOptions
            // {
            //     AbsoluteExpiration = DateTimeOffset.Now.AddMinutes(5)
            // });
            FreeSetMany<RealtimeOnline>(_client, _distributedCacheOption.KeyPrefix, realtimeOnlineKVs, new DistributedCacheEntryOptions
            {
                AbsoluteExpiration = DateTimeOffset.Now.AddMilliseconds(5)
            }, ignoreExists: true);
            sw.Stop();
            Logger.LogInformation($"GetOrAddManyAsync 执行时间：{sw.ElapsedMilliseconds} ms");
        }

        // 清理环境
        // await _realtimeOnlineCache.RemoveManyAsync(keys);
        FreeRemoveMany<RealtimeOnline>(_client, _distributedCacheOption.KeyPrefix, keys);
    }

    private void FreeRemoveMany<TCacheItem>(RedisClient client, string keyPrefix, IEnumerable<string> keys)
    {
        try
        {
            string cacheName = typeof(TCacheItem).ToString();

            var normalizedKeys = keys.Select(key => $"c:{cacheName},k:{keyPrefix}{key}").ToArray();

            client.Del(normalizedKeys);
        }
        catch (Exception ex)
        {
            throw new Exception($"FreeRemoveMany() => {ex.Message}");
        }
    }

    private void FreeSetMany<TCacheItem>(RedisClient client, string keyPrefix, IEnumerable<KeyValuePair<string, TCacheItem>> items, DistributedCacheEntryOptions options = null, bool ignoreExists = false)
    {
        try
        {
            string cacheName = typeof(TCacheItem).ToString();

            var normalizedKvs = items.Select(kv => new KeyValuePair<string, TCacheItem>(
                $"c:{cacheName},k:{keyPrefix}{kv.Key}",
                kv.Value
            )).ToDictionary(x => x.Key, x => x.Value);

            int timeoutSeconds = 0;
            if (options != null)
            {
                if (options.AbsoluteExpiration != null)
                {
                    timeoutSeconds = (int)(options.AbsoluteExpiration - DateTime.Now).Value.TotalSeconds;
                }
                else if (options.AbsoluteExpirationRelativeToNow != null)
                {
                    timeoutSeconds = (int)options.AbsoluteExpirationRelativeToNow.Value.TotalSeconds;
                }
                timeoutSeconds = timeoutSeconds > 0 ? timeoutSeconds : 0;
            }

            using (var pipe = client.StartPipe())
            {
                foreach (var kv in normalizedKvs)
                {
                    if (ignoreExists)
                    {
                        pipe.SetNx<TCacheItem>(kv.Key, kv.Value, timeoutSeconds);
                    }
                    else
                    {
                        pipe.Set<TCacheItem>(kv.Key, kv.Value, timeoutSeconds);
                    }

                }
                pipe.EndPipe();
            }
        }
        catch (Exception ex)
        {
            throw new Exception($"FreeSetMany() => {ex.Message}");
        }
    }

    private void FreeSet<TCacheItem>(RedisClient client, string keyPrefix, string key, TCacheItem value, DistributedCacheEntryOptions options = null)
    {
        try
        {
            string cacheName = typeof(TCacheItem).ToString();

            var normalizedKey = $"c:{cacheName},k:{keyPrefix}{key}";

            int timeoutSeconds = 0;
            if (options != null)
            {
                if (options.AbsoluteExpiration != null)
                {
                    timeoutSeconds = (int)(options.AbsoluteExpiration - DateTime.Now).Value.TotalSeconds;
                }
                else if (options.AbsoluteExpirationRelativeToNow != null)
                {
                    timeoutSeconds = (int)options.AbsoluteExpirationRelativeToNow.Value.TotalSeconds;
                }
                timeoutSeconds = timeoutSeconds > 0 ? timeoutSeconds : 0;
            }

            client.Set<TCacheItem>(normalizedKey, value, timeoutSeconds);
        }
        catch (Exception ex)
        {
            throw new Exception($"FreeSet() => {ex.Message}");
        }

    }

    private KeyValuePair<string, TCacheItem>[] FreeGetMany<TCacheItem>(RedisClient client, string keyPrefix, IEnumerable<string> keys)
    {
        try
        {
            string CacheName = typeof(TCacheItem).ToString();

            var normalizedKeys = keys.Select(key => $"c:{CacheName},k:{keyPrefix}{key}").ToArray();

            var values = client.MGet<TCacheItem>(normalizedKeys);

            List<KeyValuePair<string, TCacheItem>> kvs = new List<KeyValuePair<string, TCacheItem>>();
            var keysList = keys.ToList();
            for (var i = 0; i < keysList.Count; i++)
            {
                kvs.Add(new KeyValuePair<string, TCacheItem>(keysList[i], values[i]));
            }

            return kvs.ToArray();
        }
        catch (Exception ex)
        {
            throw new Exception($"FreeGetMany() => {ex.Message}");
        }
    }

    private TCacheItem FreeGet<TCacheItem>(RedisClient client, string keyPrefix, string key)
    {
        try
        {
            string cacheName = typeof(TCacheItem).ToString();

            var normalizedKey = $"c:{cacheName},k:{keyPrefix}{key}";

            var value = client.Get<TCacheItem>(normalizedKey);

            return value;
        }
        catch (Exception ex)
        {
            throw new Exception($"GetAsync() => {ex.Message}");
        }
    }
}
