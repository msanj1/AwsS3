using Amazon.S3.Model;
using Amazon.S3;

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

    public class S3Service
    {
        private readonly IAmazonS3 _s3Client;

        public S3Service(IAmazonS3 client)
        {
            _s3Client = client;
        }

        public async Task<(bool, List<S3Item>)> SelectObjectsByPrefixAsync(
            string bucketName,
            string prefix,
            CancellationToken cancellationToken = default)
        {
            string? continuationToken = null;

            var operationSuccessful = false;
            var objectsSelected = new List<S3Item>();

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
                        objectsSelected.AddRange(listResponse.S3Objects.Select(obj => new S3Item { AwsS3Key = obj.Key }));

                    continuationToken = listResponse.IsTruncated.GetValueOrDefault() ? listResponse.NextContinuationToken : null;

                } while (continuationToken != null);

                operationSuccessful = true;
            }
            catch (Exception e)
            {
                Console.WriteLine($"Error occured "+ e.Message);
            }

            return (operationSuccessful, objectsSelected);
        }

        public async Task<(bool, S3DeletionSummary)> DeleteObjectsByPrefixAsync(
            string bucketName,
            string prefix,
            CancellationToken cancellationToken = default)
        {
            Console.WriteLine($"Fetching objects with prefix: {prefix}");

            var operationSummary = new S3DeletionSummary();
            var operationSuccessful = false;
            var allKeysToDelete = new List<KeyVersion>();

            var (successful, allObjects) = await SelectObjectsByPrefixAsync(bucketName, prefix, cancellationToken);

            if (!successful)
            {
                return (operationSuccessful, operationSummary);
            }

            allKeysToDelete.AddRange(allObjects.Select(obj => new KeyVersion { Key = obj.AwsS3Key }));

            operationSummary.TotalObjectsFound = allKeysToDelete.Count;

            if (operationSummary.TotalObjectsFound == 0)
            {
                operationSuccessful = true;
                Console.WriteLine("No objects found with the specified prefix.");
                return (operationSuccessful, operationSummary);
            }

            Console.WriteLine($"Found {operationSummary.TotalObjectsFound} objects. Starting batch deletion...");

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

                    operationSummary.TotalDeleted += deleteResponse.DeletedObjects.Count;
                    if(deleteResponse.DeleteErrors != null) { }
                        operationSummary.Errors.AddRange(deleteResponse.DeleteErrors);

                    batchCount++;
                    Console.WriteLine($"Batch {batchCount}: Deleted {deleteResponse.DeletedObjects.Count} objects.");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Deletion cancelled mid-operation.");
                operationSuccessful = false;
            }

            Console.WriteLine("Deletion complete.");
            Console.WriteLine($"Summary: Found = {operationSummary.TotalObjectsFound}, Deleted = {operationSummary.TotalDeleted}, Errors = {operationSummary.Errors.Count}");

            operationSuccessful = operationSummary.Errors.Count == 0;

            return (operationSuccessful, operationSummary);
        }
    }
}
