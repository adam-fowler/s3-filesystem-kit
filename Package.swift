// swift-tools-version:5.1
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "aws-s3-filesystem-kit",
    products: [
        .library(name: "AWSS3FileSystemKit", targets: ["AWSS3FileSystemKit"]),
    ],
    dependencies: [
        .package(url: "https://github.com/swift-aws/aws-sdk-swift", .upToNextMajor(from: "4.0.0"))
    ],
    targets: [
        .target(name: "AWSS3FileSystemKit", dependencies: ["S3"]),
        .testTarget(name: "AWSS3FileSystemKitTests", dependencies: ["AWSS3FileSystemKit"]),
    ]
)
