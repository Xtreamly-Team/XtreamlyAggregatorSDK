using System.Reflection;
using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Writes;

namespace XtreamlyAggregatorSDK;

public class XtreamlyAggregator<T> : IDisposable
{
    private readonly InfluxDBClient _client;
    private readonly string _organization;
    private readonly string _aggregatorName;
    private readonly WriteApi _writeApi;

    public XtreamlyAggregator(bool gzip = true)
    {
        var influxdbAddress = Environment.GetEnvironmentVariable("influxdbAddress") ?? throw new InvalidOperationException("influxdbAddress"+ " env variable is not provided");
        _organization = Environment.GetEnvironmentVariable("organization") ?? throw new InvalidOperationException("organization"+ " env variable is not provided");
        var secret = Environment.GetEnvironmentVariable("secret") ?? throw new InvalidOperationException("secret" + " env variable is not provided");
        _aggregatorName = typeof(T).Name;
        _client = new InfluxDBClient(influxdbAddress, secret);
        var health =  _client.PingAsync().Result;
        if (!health)
        {
            throw new Exception("influxdb connection failed");
        }
        if (gzip) _client.EnableGzip();
        _writeApi = _client.GetWriteApi();
        var bucketAlreadyExists =  _client.GetBucketsApi().FindBucketsAsync(name: _aggregatorName).Result.Count >=1;
        var org =  ( _client.GetOrganizationsApi().FindOrganizationsAsync(org: _organization)).Result.FirstOrDefault()?.Id;
        if (org == null)
        {
            _client.GetOrganizationsApi().CreateOrganizationAsync(_organization);
        }
        if (bucketAlreadyExists) return;
        var retention = new BucketRetentionRules(BucketRetentionRules.TypeEnum.Expire, int.MaxValue);
        _client.GetBucketsApi().CreateBucketAsync(_aggregatorName, retention, org).Wait();
    }

    public Task Send( T data)
    {
        return Task.Run(() =>
        {
            _writeApi.WriteMeasurement(data,WritePrecision.Ms, bucket: _aggregatorName, org: _organization);
        });
    }
    
    public Task SendMany( T[] data)
    {

        return Task.Run(() =>
        {
            _writeApi.WriteMeasurements(data,WritePrecision.Ms, bucket: _aggregatorName, org: _organization);
          
        });
       
    }

    public void Dispose()
    {
        _client.Dispose();
        _writeApi.Dispose();
    }
}