//
// Created by yyh-qisima on 2023/9/28.
//

#ifndef HELLO_S3_S3_OPRS_H
#define HELLO_S3_S3_OPRS_H

#include <iostream>
#include <fstream>
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/BucketLocationConstraint.h>
#include <aws/core/utils/UUID.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/s3/model/PutBucketPolicyRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include "utils.h"

#include <sstream>
#include <iomanip>
#include <chrono>

bool ListS3Bucket(const std::shared_ptr<Aws::S3::S3Client>& s3Client) {

    /* list the buckets from S3 */
    auto outcome = s3Client->ListBuckets();
    if (!outcome.IsSuccess()) {
        std::cerr << "Failed with error: " << outcome.GetError() << std::endl;
        return false;
    } else {
        std::cout << "Found " << outcome.GetResult().GetBuckets().size()
                  << " buckets\n";
        for (auto &bucket: outcome.GetResult().GetBuckets()) {
            auto date = bucket.GetCreationDate();
            std::cout << bucket.GetName() << "(" << date.ToGmtString("%Y-%m-%d-%H-%M-%S") << ")" << std::endl;
        }
        return true;
    }

}


/* put policy to bucket */
bool PutBucketPolicy(const Aws::String &bucketName,
                     const Aws::String &policyBody,
                     const std::shared_ptr<Aws::S3::S3Client>& s3_client) {
    std::shared_ptr<Aws::StringStream> request_body =
            Aws::MakeShared<Aws::StringStream>("");
    *request_body << policyBody;

    Aws::S3::Model::PutBucketPolicyRequest request;
    request.SetBucket(bucketName);
    request.SetBody(request_body);

    Aws::S3::Model::PutBucketPolicyOutcome outcome =
            s3_client->PutBucketPolicy(request);

    if (!outcome.IsSuccess()) {
        std::cerr << "Error: PutBucketPolicy: "
                  << outcome.GetError().GetMessage() << std::endl;
    }
    else {
        std::cout << "Set the following policy body for the bucket '" <<
                  bucketName << "':" << std::endl << std::endl;
        std::cout << policyBody << std::endl;
    }

    return outcome.IsSuccess();
}


/* create a new bucket to S3 */
bool CreateBucket(const Aws::String &bucketName,
                  const std::shared_ptr<Aws::S3::S3Client>& client) {
    Aws::S3::Model::CreateBucketRequest request;
    request.SetBucket(bucketName);

//    if (clientConfig.region != DEFAULT_REGION) {
//        Aws::S3::Model::CreateBucketConfiguration createBucketConfig;
//        createBucketConfig.SetLocationConstraint(
//                Aws::S3::Model::BucketLocationConstraintMapper::GetBucketLocationConstraintForName(
//                        clientConfig.region));
//        request.SetCreateBucketConfiguration(createBucketConfig);
//    }

    Aws::S3::Model::CreateBucketOutcome outcome = client->CreateBucket(request);
    if (!outcome.IsSuccess()) {
        const auto& err = outcome.GetError();
        std::cerr << "Error: CreateBucket: " <<
                  err.GetExceptionName() << ": " << err.GetMessage() << std::endl;
    }
    else {
        std::cout << "Created bucket " << bucketName <<
                  " in the specified AWS Region." << std::endl;
    }

    return outcome.IsSuccess();
}

/* delete the empty bucket */
bool DeleteBucket(const Aws::String &bucketName,
                  const std::shared_ptr<Aws::S3::S3Client>& client) {

    Aws::S3::Model::DeleteBucketRequest request;
    request.SetBucket(bucketName);

    Aws::S3::Model::DeleteBucketOutcome outcome =
            client->DeleteBucket(request);

    if (!outcome.IsSuccess()) {
        const Aws::S3::S3Error &err = outcome.GetError();
        std::cerr << "Error: DeleteBucket: " <<
                  err.GetExceptionName() << ": " << err.GetMessage() << std::endl;
    }
    else {
        std::cout << "The bucket was deleted" << std::endl;
    }

    return outcome.IsSuccess();
}

/* upload a file as an object to target bucket */
bool PutObject(const Aws::String &bucketName,
               const Aws::String &fileName,
               const std::shared_ptr<Aws::S3::S3Client>& s3_client) {

    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(bucketName);
    //We are using the name of the file as the key for the object in the bucket.
    //However, this is just a string and can be set according to your retrieval needs.
    request.SetKey(fileName);

    std::shared_ptr<Aws::IOStream> inputData =
            Aws::MakeShared<Aws::FStream>("SampleAllocationTag",
                                          fileName.c_str(),
                                          std::ios_base::in | std::ios_base::binary);

    if (!*inputData) {
        std::cerr << "Error unable to read file " << fileName << std::endl;
        return false;
    }

    /* file content as the value */
    request.SetBody(inputData);

    Aws::S3::Model::PutObjectOutcome outcome = s3_client->PutObject(request);

    if (!outcome.IsSuccess()) {
        std::cerr << "Error: PutObject: " <<
                  outcome.GetError().GetMessage() << std::endl;
    }
    else {
        std::cout << "Added object '" << fileName << "' to bucket '"
                  << bucketName << "'.";
    }

    return outcome.IsSuccess();
}

/* list the objects in the target bucket */
bool ListObjects(const Aws::String &bucketName,
                 const std::shared_ptr<Aws::S3::S3Client>& s3_client) {
    Aws::S3::Model::ListObjectsRequest request;
    request.WithBucket(bucketName);

    auto outcome = s3_client->ListObjects(request);

    if (!outcome.IsSuccess()) {
        std::cerr << "Error: ListObjects: " <<
                  outcome.GetError().GetMessage() << std::endl;
    }
    else {
        Aws::Vector<Aws::S3::Model::Object> objects =
                outcome.GetResult().GetContents();

        for (Aws::S3::Model::Object &object: objects) {
            std::cout << object.GetKey() << std::endl;
        }
    }

    return outcome.IsSuccess();
}

bool DeleteObject(const Aws::String &objectKey,
                  const Aws::String &fromBucket,
                  const std::shared_ptr<Aws::S3::S3Client>& client) {
    Aws::S3::Model::DeleteObjectRequest request;

    request.
        WithKey(objectKey).
        WithBucket(fromBucket);

    Aws::S3::Model::DeleteObjectOutcome outcome =
            client->DeleteObject(request);

    if (!outcome.IsSuccess()) {
        const auto& err = outcome.GetError();
        std::cerr << "Error: DeleteObject: " <<
                  err.GetExceptionName() << ": " << err.GetMessage() << std::endl;
    }
    else {
        std::cout << "Successfully deleted the object." << std::endl;
    }

    return outcome.IsSuccess();
}

/* find whether an object in the target bucket */
bool ContainsObject(const Aws::String &objectKey,
                           const Aws::String &fromBucket,
                    const std::shared_ptr<Aws::S3::S3Client>& client) {
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(fromBucket);
    request.SetKey(objectKey);

    Aws::S3::Model::GetObjectOutcome outcome =
            client->GetObject(request);

    if (!outcome.IsSuccess()) {
        const Aws::S3::S3Error &err = outcome.GetError();
        std::cerr << "Error: GetObject: " <<
                  err.GetExceptionName() << ": " << err.GetMessage() << std::endl;
    }
    else {
        std::cout << "Successfully retrieved '" << objectKey << "' from '"
                  << fromBucket << "'." << std::endl;
    }
    return outcome.IsSuccess();
}


/* 3. Download the object to a local file. */
void GetObjectFile(const Aws::String& key,
                   const Aws::String& saveFilePath,
                   const Aws::String& bucketName,
                   const std::shared_ptr<Aws::S3::S3Client>& client) {
    {
        Aws::S3::Model::GetObjectRequest request;
        request.SetBucket(bucketName);
        request.SetKey(key);

        Aws::S3::Model::GetObjectOutcome outcome =
                client->GetObject(request);

        if (!outcome.IsSuccess()) {
            const Aws::S3::S3Error &err = outcome.GetError();
            std::cerr << "Error: GetObject: " <<
                      err.GetExceptionName() << ": " << err.GetMessage() << std::endl;
        }
        else {
            std::cout << "Downloaded the object with the key, '" << key << "', in the bucket, '"
                      << bucketName << "'." << std::endl;

            Aws::IOStream &ioStream = outcome.GetResultWithOwnership().
                    GetBody();
            Aws::OFStream outStream(saveFilePath, std::ios_base::out | std::ios_base::binary);
            if (!outStream.is_open()) {
                std::cout << "Error: unable to open file, '" << saveFilePath << "'." << std::endl;
            }
            else {
                outStream << ioStream.rdbuf();
                std::cout << "Wrote the downloaded object to the file '"
                          << saveFilePath << "'." << std::endl;
            }
        }
    }
}


#endif //HELLO_S3_S3_OPRS_H
