using Amazon;
using Amazon.S3;
using AmazonS3;

var client = new AmazonS3Client(RegionEndpoint.APSoutheast2);

var s3Service = new S3Service(client);

using var cts = new CancellationTokenSource();

var result = await s3Service.DeleteObjectsByPrefixAsync("mohsenawsbucket", "solution.java", cts.Token);
