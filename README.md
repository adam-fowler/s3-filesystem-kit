# S3 File System Kit

[<img src="http://img.shields.io/badge/swift-5.1-brightgreen.svg" alt="Swift 5.1" />](https://swift.org)
[<img src="https://github.com/adam-fowler/s3-filesystem-kit/workflows/CI/badge.svg" alt="CI" />](https://github.com/adam-fowler/s3-filesystem-kit/actions?query=workflow%3ACI)

File manager for Amazon Web Service S3.

# Setup

S3 File System uses the S3 library from [Soto](https://github.com/soto-project/soto). You need to initialise `S3FileSystem` with a `S3` client object from this library. S3 File System will require AWS credentials before you can continue. The `S3` client object will provide these. 

```swift
let awsClient = AWSClient(httpClientProvider: .createNew)
let s3 = S3(client: awsClient, region: .euwest1)
let s3fs = S3FileSystem(s3Client: s3)
```

# Path descriptors

S3 File System uses an `S3File` to describe the location of a file in S3 and an `S3Folder` to describe the location of a folder. These are initialized with a url of the form `s3://<bucketname>/<path>`. For example
```swift
let folder = S3Folder(url: "s3://bucket/folder")
let file = S3File(url: "s3://bucket2/folder/file")
```
Most functions in `S3FileSystem` have two forms, one that takes an `S3File` and one that takes a filename String relative to the currentPath set in `S3FileSystem`.
```swift
s3fs.writeFile(S3File("s3://bucket/folder/file")!, data: data)
```
and
```swift
s3fs.setCurrentFolder(S3Folder(url: "s3://bucket/folder")!)
    .flatMap { _ in
        return s3fs.writeFile(name: "file", data: data)
}
```
will both do the same thing. Except in the second case `setCurrentFolder` will check the S3 bucket exists before running `writeFile`. The advantage of the second version is you can now push and pop folders (using `pushFolder` and `popFolder`) and traverse the S3 bucket as if it is a hierarchical file system. 

# Asynchronous

Most of the functions in `S3FileSystem` return an `EventLoopFuture` from the swift-nio library. This is not the result of the function. This is populated with the result when it is available. In this manner the library will not block the main thread. It is recommended you familiarize yourself with swift-nio [documention](https://apple.github.io/swift-nio/docs/current/NIO/index.html) to get the most out of S3 File System. 

The recommended way to interact with `EventLoopFutures` is chaining. The following creates an S3 bucket, uploads an object and then downloads it.
```swift
import S3FileSystemKit

let result: EventLoopFuture<Data> = s3fs.setCurrentFolder(S3Folder(url: "s3://bucket/")!, createBucket: true)
  .flatMap { _ in
      return self.s3fs.writeFile(name: filename, data: data)
  }
  .flatMap { _ in
      return self.s3fs.readFile(name: filename)
  }
```

# Documentation

You can find API reference documentation [here](https://adam-fowler.github.io/s3-filesystem-kit/index.html). It is also useful to read the Amazon S3 documentation which you can find [here](https://docs.aws.amazon.com/AmazonS3/latest/dev/Welcome.html).
