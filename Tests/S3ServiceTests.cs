using Xunit;
using FluentAssertions;
using Moq;
using Amazon.S3;
using Amazon.S3.Model;
using System.Threading;

namespace AmazonS3.Tests
{
    public class S3ServiceTests
    {
        private readonly Mock<IAmazonS3> _s3ClientMock;
        private readonly S3Service _s3Service;

        public S3ServiceTests()
        {
            _s3ClientMock = new Mock<IAmazonS3>();
            _s3Service = new S3Service(_s3ClientMock.Object);
        }

        [Fact]
        public async Task SelectObjectsByPrefixAsync_WhenObjectsExist_ReturnsSuccessAndObjects()
        {
            // Arrange
            var bucketName = "test-bucket";
            var prefix = "test-prefix/";
            var expectedObjects = new List<S3Object>
            {
                new() { Key = "test-prefix/file1.txt" },
                new() { Key = "test-prefix/file2.txt" }
            };

            _s3ClientMock
                .Setup(x => x.ListObjectsV2Async(
                    It.Is<ListObjectsV2Request>(r => 
                        r.BucketName == bucketName && 
                        r.Prefix == prefix),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ListObjectsV2Response
                {
                    S3Objects = expectedObjects,
                    IsTruncated = false
                });

            // Act
            var (success, result) = await _s3Service.SelectObjectsByPrefixAsync(bucketName, prefix);

            // Assert
            success.Should().BeTrue();
            result.Should().HaveCount(2);
            result.Select(x => x.AwsS3Key).Should().BeEquivalentTo(
                expectedObjects.Select(x => x.Key));
        }

        [Fact]
        public async Task SelectObjectsByPrefixAsync_WhenNoObjectsExist_ReturnsEmptyList()
        {
            // Arrange
            var bucketName = "test-bucket";
            var prefix = "empty-prefix/";

            _s3ClientMock
                .Setup(x => x.ListObjectsV2Async(
                    It.IsAny<ListObjectsV2Request>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ListObjectsV2Response
                {
                    S3Objects = new List<S3Object>(),
                    IsTruncated = false
                });

            // Act
            var (success, result) = await _s3Service.SelectObjectsByPrefixAsync(bucketName, prefix);

            // Assert
            success.Should().BeTrue();
            result.Should().BeEmpty();
        }

        [Fact]
        public async Task SelectObjectsByPrefixAsync_WhenPaginated_ReturnsAllObjects()
        {
            // Arrange
            var bucketName = "test-bucket";
            var prefix = "test-prefix/";
            var firstPageObjects = new List<S3Object>
            {
                new() { Key = "test-prefix/file1.txt" },
                new() { Key = "test-prefix/file2.txt" }
            };
            var secondPageObjects = new List<S3Object>
            {
                new() { Key = "test-prefix/file3.txt" },
                new() { Key = "test-prefix/file4.txt" }
            };

            _s3ClientMock
                .SetupSequence(x => x.ListObjectsV2Async(
                    It.IsAny<ListObjectsV2Request>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ListObjectsV2Response
                {
                    S3Objects = firstPageObjects,
                    IsTruncated = true,
                    NextContinuationToken = "token"
                })
                .ReturnsAsync(new ListObjectsV2Response
                {
                    S3Objects = secondPageObjects,
                    IsTruncated = false
                });

            // Act
            var (success, result) = await _s3Service.SelectObjectsByPrefixAsync(bucketName, prefix);

            // Assert
            success.Should().BeTrue();
            result.Should().HaveCount(4);
            result.Select(x => x.AwsS3Key).Should().BeEquivalentTo(
                firstPageObjects.Concat(secondPageObjects).Select(x => x.Key));
        }

        [Fact]
        public async Task SelectObjectsByPrefixAsync_WhenExceptionOccurs_ReturnsFailureAndEmptyList()
        {
            // Arrange
            var bucketName = "test-bucket";
            var prefix = "test-prefix/";

            _s3ClientMock
                .Setup(x => x.ListObjectsV2Async(
                    It.IsAny<ListObjectsV2Request>(),
                    It.IsAny<CancellationToken>()))
                .ThrowsAsync(new AmazonS3Exception("Test exception"));

            // Act
            var (success, result) = await _s3Service.SelectObjectsByPrefixAsync(bucketName, prefix);

            // Assert
            success.Should().BeFalse();
            result.Should().BeEmpty();
        }

        [Fact]
        public async Task SelectObjectsByPrefixAsync_WhenCancelled_ThrowsOperationCanceledException()
        {
            // Arrange
            var bucketName = "test-bucket";
            var prefix = "test-prefix/";
            var cts = new CancellationTokenSource();
            cts.Cancel();

            // Act
            var (success, result) = await _s3Service.SelectObjectsByPrefixAsync(bucketName, prefix);

            // Assert
            success.Should().BeFalse();
            result.Should().BeEmpty();
        }
    }
} 