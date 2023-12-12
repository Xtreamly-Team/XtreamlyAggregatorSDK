using System.Reflection;
using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Writes;

namespace XtreamlyAggregatorSDK;

public class XtreamlyAggregator : IDisposable
{
    private readonly InfluxDBClient _client;
    private readonly string _organization;
    private readonly string _aggregatorName;
    private readonly WriteApi _writeApi;

    public XtreamlyAggregator(bool gzip)
    {
        var influxdbAddress = Environment.GetEnvironmentVariable("influxdbAddress".ToUpper()) ?? throw new InvalidOperationException("influxdbAddress".ToUpper() + " env variable is not provided");
        _organization = Environment.GetEnvironmentVariable("organization".ToUpper()) ?? throw new InvalidOperationException("organization".ToUpper() + " env variable is not provided");
        var secret = Environment.GetEnvironmentVariable("organization".ToUpper()) ?? throw new InvalidOperationException("organization".ToUpper() + " env variable is not provided");
        _aggregatorName =  Environment.GetEnvironmentVariable("aggregatorName".ToUpper()) ?? throw new InvalidOperationException("aggregatorName".ToUpper() + " env variable is not provided");
        _client = new InfluxDBClient(influxdbAddress, secret);
        var health =  _client.PingAsync().Result;
        if (!health)
        {
            throw new Exception("influxdb connection failed");
        }
        if (gzip) _client.EnableGzip();
        _writeApi = _client.GetWriteApi();
        
        var orgId = ( _client.GetOrganizationsApi().FindOrganizationsAsync(org: _organization)).Result.FirstOrDefault()?.Id;
        if (orgId != null) return;
        var retention = new BucketRetentionRules(BucketRetentionRules.TypeEnum.Expire, int.MaxValue);
        _client.GetBucketsApi().CreateBucketAsync(_aggregatorName, retention, orgId).Wait();
    }

    public Task SendDataPoint( AbstractAggregationPoint data, string measurement = "Point" )
    {
        return Task.Run(() =>
        {
            var type = data.GetType();
            var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public);
            var point = PointData.Measurement(measurement);
            point = properties.Aggregate(point, (current, property) => current.Tag(property.Name, (property.GetValue(data) ?? string.Empty).ToString()));
            _writeApi.WritePoint(point, _aggregatorName, _organization);
        });
    }
    
    public Task SendDataPoints( AbstractAggregationPoint[] data, string measurement = "Point" )
    {

        return Task.Run(() =>
        {
            var final = new List<PointData>();
            foreach (var dataPoints in data)
            {
                var type = dataPoints.GetType();
                var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public);
                var point = PointData.Measurement(measurement);
                point = properties.Aggregate(point, (current, property) => current.Tag(property.Name, (property.GetValue(data) ?? string.Empty).ToString()));
                final.Add(point);
            }
            _writeApi.WritePoints(final, _aggregatorName, _organization);
        });
       
    }

    public void Dispose()
    {
        _client.Dispose();
        _writeApi.Dispose();
    }
}