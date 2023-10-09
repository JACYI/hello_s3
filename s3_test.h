#include <iostream>
#include <memory>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include "s3_oprs.h"
#include "utils.h"
#include "s3file_io.h"
using namespace Aws;
using namespace Aws::Auth;

int s3_fs_test() {
//    auto *t = new S3FSFileIO<int>
    S3FSFileIO<std::shared_ptr<::arrow::fs::FileSystem>> io;
}

int test_s3() {
    Aws::SDKOptions options;
    // Optionally change the log level for debugging.
//   options.loggingOptions.logLevel = Utils::Logging::LogLevel::Debug;
    Aws::InitAPI(options); // Should only be called once.
    int result = 0;
    {
        Aws::Client::ClientConfiguration clientConfig;
        // Optional: Set to the AWS Region (overrides config file).
        clientConfig.region = DEFAULT_REGION;

        // You don't normally have to test that you are authenticated. But the S3 service permits anonymous requests, thus the s3Client will return "success" and 0 buckets even if you are unauthenticated, which can be confusing to a new user.
        auto provider = Aws::MakeShared<DefaultAWSCredentialsProviderChain>("alloc-tag");
        auto creds = provider->GetAWSCredentials();
        if (creds.IsEmpty()) {
            std::cerr << "Failed authentication" << std::endl;
        }
        // create unique client
        std::shared_ptr<Aws::S3::S3Client> client =
                std::make_shared<Aws::S3::S3Client>(clientConfig);

        // 1. list the S3 buckets
        ListS3Bucket(client);

        // 2. create a new bucket
        String bucket_name = "yonghao-yi-created-by-code";
        if(!CreateBucket(bucket_name, client)) {
            return 0;
        }
        ListS3Bucket(client);

        /* 3. create a new policy and put it to target bucket */
//        auto policy = GetPolicyString(USER_ARN, bucket_name);
//        PutBucketPolicy(bucket_name, "{}", clientConfig);
//        ListS3Bucket(clientConfig);

        /* 5. add object to target bucket */
        const Aws::String testfile = "/Users/yyh-qisima/CLionProjects/AWSSDKTest/testfile1.txt";
        if(!PutObject(bucket_name, testfile, client))
            return 0;

        /* 6. list the objects */
        if(!ListObjects(bucket_name, client)) {
            return 0;
        }

        /* 7. get the object from a bucket */
        const Aws::String download_filepath = "./test_download.txt";
        GetObjectFile(testfile, download_filepath, bucket_name, client);

        /* 8. delete an object */
        if(!DeleteObject(testfile, bucket_name, client))
            return 0;
        ListObjects(bucket_name, client);


        /* 4. delete the empty bucket with name */
        if(!DeleteBucket(bucket_name, client)) {
            return 0;
        }
        ListS3Bucket(client);

    }

    Aws::ShutdownAPI(options); // Should only be called once.

    return result;
}