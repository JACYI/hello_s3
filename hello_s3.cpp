#include <iostream>
#include <memory>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include "s3_oprs.h"
#include "utils.h"
#include "s3_test.h"
using namespace Aws;
using namespace Aws::Auth;

/*
 *  A "Hello S3" starter application which initializes an Amazon Simple Storage Service (Amazon S3) client
 *  and lists the Amazon S3 buckets in the selected region.
 *
 *  main function
 *
 *  Usage: 'hello_s3'
 *
 */



int main(int argc, char **argv) {
    test_s3();
    s3_fs_test();
}
