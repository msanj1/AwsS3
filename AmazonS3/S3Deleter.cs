using Amazon.S3.Model;
using Amazon.S3;
using Amazon;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AmazonS3
{
    public class S3DeletionSummary
    {
        public int TotalObjectsFound { get; set; }
        public int TotalDeleted { get; set; }
        public List<DeleteError> Errors { get; set; } = new();
    }

    public class S3Item
    {
        public required string AwsS3Key { get; set; }
    }

    public class Operation<T>
        where T: new()
    {
        public bool Successful { get; set; }
        public T Item { get; set; }
    }

    public class S3Deleter
    {
        private readonly IAmazonS3 _s3Client;

        public S3Deleter(RegionEndpoint region)
        {
            _s3Client = new AmazonS3Client(region);
        }

        public async Task<Operation<List<S3Item>>> SelectObjectsByPrefixAsync(
            string bucketName,
            string prefix,
            CancellationToken cancellationToken = default)
        {
            string? continuationToken = null;

            var operation = new Operation<List<S3Item>>();
            operation.Item = new List<S3Item>();
            try
            {
                do
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var listRequest = new ListObjectsV2Request
                    {
                        BucketName = bucketName,
                        Prefix = prefix,
                        ContinuationToken = continuationToken
                    };

                    var listResponse = await _s3Client.ListObjectsV2Async(listRequest, cancellationToken);
                    if (listResponse.S3Objects != null)
                        operation.Item.AddRange(listResponse.S3Objects.Select(obj => new S3Item { AwsS3Key = obj.Key }));

                    continuationToken = listResponse.IsTruncated.GetValueOrDefault() ? listResponse.NextContinuationToken : null;

                } while (continuationToken != null);
                operation.Successful = true;
            }
            catch (Exception e)
            {
                return operation;
            }

            return operation;
        }

        public async Task<S3DeletionSummary> DeleteObjectsByPrefixAsync(
            string bucketName,
            string prefix,
            CancellationToken cancellationToken = default)
        {
            Console.WriteLine($"Fetching objects with prefix: {prefix}");

            var allKeysToDelete = new List<KeyVersion>();

            var allObjects = await SelectObjectsByPrefixAsync(bucketName, prefix, cancellationToken);

            if (!allObjects.Successful)
            {
                return new S3DeletionSummary();
            }

            allKeysToDelete.AddRange(allObjects.Item.Select(obj => new KeyVersion { Key = obj.AwsS3Key }));

            var summary = new S3DeletionSummary
            {
                TotalObjectsFound = allKeysToDelete.Count
            };

            if (summary.TotalObjectsFound == 0)
            {
                Console.WriteLine("No objects found with the specified prefix.");
                return summary;
            }

            Console.WriteLine($"Found {summary.TotalObjectsFound} objects. Starting batch deletion...");

            const int batchSize = 1000;
            int batchCount = 0;

            try
            {
                foreach (var batch in allKeysToDelete.Chunk(batchSize))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var deleteRequest = new DeleteObjectsRequest
                    {
                        BucketName = bucketName,
                        Objects = batch.ToList()
                    };

                    var deleteResponse = await _s3Client.DeleteObjectsAsync(deleteRequest, cancellationToken);

                    summary.TotalDeleted += deleteResponse.DeletedObjects.Count;
                    if(deleteResponse.DeleteErrors != null)
                      summary.Errors.AddRange(deleteResponse.DeleteErrors);

                    batchCount++;
                    Console.WriteLine($"Batch {batchCount}: Deleted {deleteResponse.DeletedObjects.Count} objects.");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Deletion cancelled mid-operation.");
            }

            Console.WriteLine("Deletion complete.");
            Console.WriteLine($"Summary: Found = {summary.TotalObjectsFound}, Deleted = {summary.TotalDeleted}, Errors = {summary.Errors.Count}");

            return summary;
        }
    }
}
