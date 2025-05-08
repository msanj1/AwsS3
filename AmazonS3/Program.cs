using Amazon;
using AmazonS3;

var deleter = new S3Deleter(RegionEndpoint.APSoutheast2);
using var cts = new CancellationTokenSource();

var result = await deleter.DeleteObjectsByPrefixAsync("mohsenawsbucket", "0f7358c7-e00d-4f0e-9611-3e585bb6dd9e", cts.Token);
