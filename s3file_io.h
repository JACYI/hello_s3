//
// Created by yyh-qisima on 2023/10/7.
//
#include <iostream>
#include <map>
#include <arrow/io/interfaces.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/result.h>
#include <exception>
using namespace arrow;
#ifndef HELLO_S3_S3FILE_IO_H
#define HELLO_S3_S3FILE_IO_H

template<typename T>
class S3FSFileIO {

public:
    explicit S3FSFileIO() {
        Result<T> arrow_fs = ::arrow::fs::FileSystemFromUri("s3://");
        if(arrow_fs.ok())
            arrow_fs_ = arrow_fs.MoveValueUnsafe();
    }
    ~S3FSFileIO() { arrow_fs_.reset(); }

    std::shared_ptr<arrow::fs::FileInfo> GetFileInfo(const std::string &path) {
        auto ret = arrow_fs_->GetFileInfo(path);
        if(ret.ok())
            return std::make_shared<::fs::FileInfo>(ret.MoveValueUnsafe());
    }

    std::shared_ptr<::arrow::io::InputStream> newInputFile(const std::string &path) {
        auto is = arrow_fs_->OpenInputStream(path).MoveValueUnsafe();
        return is;
    }

    std::shared_ptr<::arrow::io::OutputStream> newOutputFile(const std::string &path) {
        auto os = arrow_fs_->OpenOutputStream(path).MoveValueUnsafe();
        return os;
    }

    bool deleteFile(const std::string &path) {
        return arrow_fs_->DeleteFile(path).ok();
    }

    bool renameFile(const std::string &oldLocation, const std::string &newLocation) {
        return arrow_fs_->Move(oldLocation, newLocation).ok();
    }

    bool fileExists(const std::string &location) {
        auto ret = arrow_fs_->GetFileInfo(location);
        return ret->IsDirectory() || ret->IsFile();
    }

    bool createDir(const std::string &path) {
        auto ret = arrow_fs_->CreateDir(path);
        return ret.ok();
    }

private:
//    std::shared_ptr<::arrow::fs::S3FileSystem> arrow_file_io_;
    std::shared_ptr<T> arrow_fs_;
};

#endif //HELLO_S3_S3FILE_IO_H
