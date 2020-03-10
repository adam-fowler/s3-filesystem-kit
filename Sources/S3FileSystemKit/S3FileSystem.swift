//
// name: S3FileSystem.swift
// author: Adam Fowler
// date: 2020/03/08
import Foundation
import NIO
import S3

public enum S3FileSystemError: Error {
    case invalidAction
    case accessDenied
    case bucketDoesNotExist
    case fileDoesNotExist
    case unexpected
}

/// S3 file system object
public class S3FileSystem {
    
    //MARK: Member variables
    
    /// s3 client
    let s3: S3
    /// current folder where actions will take place
    public private(set) var currentFolder: S3Folder?
    
    //MARK: Initializer
    
    /// initializer
    /// - Parameter s3Client: s3 client to use
    public init(_ s3Client: S3) {
        self.s3 = s3Client
        self.currentFolder = nil
    }
    
    //MARK: Member functions
    
    /// Set current folder to work from. When you call this function it will verify the bucket in the path exists or create it you require that
    /// - Parameters:
    ///   - folder: S3 folder descriptor
    ///   - createBucket: Create bucket if it doesnt exist
    public func setCurrentFolder(_ folder: S3Folder, createBucket: Bool = false) -> EventLoopFuture<Void> {
        let bucketFuture: EventLoopFuture<Void>
        if createBucket {
            bucketFuture = self.createBucket(bucketName: folder.bucket).map { return }
        } else {
            bucketFuture = self.headBucket(bucketName: folder.bucket).map { return }
        }
        return bucketFuture.map {
            self.currentFolder = folder
        }
    }
    
    /// Set current folder within current selected bucket
    /// - Parameter folder: folder name
    public func setCurrentFolder(_ folder: String) throws {
        guard let currentFolder = currentFolder else { throw S3FileSystemError.invalidAction }
        self.currentFolder = S3Folder(bucket: currentFolder.bucket, path: folder)
    }
    
    /// Enter subfolder relative to the current folder
    /// - Parameter folder: local folder name
    public func pushFolder(_ folder: String) throws {
        guard let folder = self.currentFolder?.subFolder(folder) else { throw S3FileSystemError.invalidAction }
        self.currentFolder = folder
    }
    
    /// Set current folder up one folder level
    public func popFolder() throws {
        guard let folder = self.currentFolder?.parent() else { throw S3FileSystemError.invalidAction }
        self.currentFolder = folder
    }
    
    /// List files in current folder
    /// - Parameter includeSubFolders: should files in subfolders be included in the list
    public func listFiles(includeSubFolders: Bool = false) -> EventLoopFuture<[S3File]> {
        guard let currentFolder = currentFolder else { return makeFailedFuture(S3FileSystemError.invalidAction) }
        return list(includeSubFolders: includeSubFolders) { response in
            guard let contents = response.contents else { return [] }
            return contents.compactMap {
                return $0.key != nil ? S3File(bucket: currentFolder.bucket, path: $0.key!) : nil
            }
        }.map { paths in
            return paths.compactMap { $0 as? S3File }
        }
    }
    
    /// List subfolders of current folder
    public func listSubfolders() -> EventLoopFuture<[S3Folder]> {
        guard let currentFolder = currentFolder else { return makeFailedFuture(S3FileSystemError.invalidAction) }
        return list { response in
            guard let subFolders = response.commonPrefixes else { return [] }
            return subFolders.compactMap {
                return $0.prefix != nil ? S3Folder(bucket: currentFolder.bucket, path: $0.prefix!) : nil
            }
        }.map { paths in
            return paths.compactMap { $0 as? S3Folder }
        }
    }
    
    /// Read file into Data
    /// - Parameter name: name of file in current folder
    public func readFile(name: String) -> EventLoopFuture<Data> {
        guard let file = currentFolder?.file(name) else { return makeFailedFuture(S3FileSystemError.invalidAction) }
        return readFile(file)
    }
    
    /// Read file into Data
    /// - Parameter file: s3 file descriptor
    public func readFile(_ file: S3File) -> EventLoopFuture<Data> {
        let request = S3.GetObjectRequest(bucket: file.bucket, key: file.path)
        return s3.getObject(request)
            .flatMapThrowing { response in
                guard let body = response.body else { throw S3FileSystemError.unexpected}
                return body
            }
            .flatMapErrorThrowing { error in
                switch error {
                case S3FileSystemError.unexpected:
                    throw error
                default:
                    throw self.convertS3Errors(error)
                }
        }
    }
    
    /// Write data to file
    /// - Parameters:
    ///   - name: file name in current folder
    ///   - data: data to be written to file
    public func writeFile(name: String, data: Data) -> EventLoopFuture<Void> {
        guard let file = currentFolder?.file(name) else { return makeFailedFuture(S3FileSystemError.invalidAction) }
        return writeFile(file, data: data)
    }
    
    /// Write data to file
    /// - Parameters:
    ///   - file: s3 file descriptor
    ///   - data: data to be written to file
    public func writeFile(_ file: S3File, data: Data) -> EventLoopFuture<Void> {
        let request = S3.PutObjectRequest(body: data, bucket: file.bucket, key: file.path)
        return s3.putObject(request)
            .map { _ in return }
            .flatMapErrorThrowing { error in
                throw self.convertS3Errors(error)
        }
    }
    
    /// Delete file
    /// - Parameter name: file name in current folder
    public func deleteFile(name: String) -> EventLoopFuture<Void> {
        guard let file = currentFolder?.file(name) else { return makeFailedFuture(S3FileSystemError.invalidAction) }
        return deleteFile(file)
    }
    
    /// Delete file
    /// - Parameter file: s3 file descriptor
    public func deleteFile(_ file: S3File) -> EventLoopFuture<Void> {
        let request = S3.DeleteObjectRequest(bucket: file.bucket, key: file.path)
        return s3.deleteObject(request)
            .map { _ in return }
            .flatMapErrorThrowing { error in
                throw self.convertS3Errors(error)
        }

    }
    
    /// Get file size
    /// - Parameter name: file name in current folder
    public func getFileSize(name: String) -> EventLoopFuture<Int64> {
        guard let file = currentFolder?.file(name) else { return makeFailedFuture(S3FileSystemError.invalidAction) }
        return getFileSize(file)
    }
    
    /// Get file size
    /// - Parameter file: s3 file descriptor
    public func getFileSize(_ file: S3File) -> EventLoopFuture<Int64> {
        let request = S3.HeadObjectRequest(bucket: file.bucket, key: file.path)
        return s3.headObject(request)
            .map { response in
                return response.contentLength ?? 0
            }
            .flatMapErrorThrowing { error in
                throw self.convertS3Errors(error)
        }

    }

    /// Return if an S3  bucket exists
    /// - Parameter bucketName: name of bucket
    public func doesBucketExist(bucketName: String) -> EventLoopFuture<Bool> {
        let request = S3.HeadBucketRequest(bucket: bucketName)
        return s3.headBucket(request)
            .map { return true }
            .flatMapErrorThrowing { error in
                return false
        }
    }
    
    /// Create an S3 bucket
    /// - Parameter bucketName: name of bucket
    public func createBucket(bucketName: String) -> EventLoopFuture<Void> {
        let request = S3.CreateBucketRequest(bucket: bucketName)
        return s3.createBucket(request)
            .map { _ in return }
            .flatMapErrorThrowing { error in
                throw self.convertS3Errors(error)
        }
    }
    
    /// Delete an S3 bucket
    /// - Parameter bucketName: name of bucket
    public func deleteBucket(bucketName: String) -> EventLoopFuture<Void> {
        if currentFolder?.bucket == bucketName {
            currentFolder = nil
        }
        let request = S3.DeleteBucketRequest(bucket: bucketName)
        return s3.deleteBucket(request)
            .flatMapErrorThrowing { error in
                throw self.convertS3Errors(error)
        }
    }
    
}

extension S3FileSystem {
    func headBucket(bucketName: String) -> EventLoopFuture<Void> {
        let request = S3.HeadBucketRequest(bucket: bucketName)
        return s3.headBucket(request)
            .map { return }
            .flatMapErrorThrowing { error in
                throw S3FileSystemError.bucketDoesNotExist
        }
    }
    
    func convertS3Errors(_ error: Error) -> Error {
        switch error {
        case S3ErrorType.noSuchBucket:
            return S3FileSystemError.bucketDoesNotExist
        case S3ErrorType.noSuchKey:
            return S3FileSystemError.fileDoesNotExist
        default:
            print("\(error)")
            return S3FileSystemError.accessDenied
        }
    }
    
    func list(includeSubFolders: Bool = false, _ collate: @escaping (S3.ListObjectsV2Output) -> [S3Path]) -> EventLoopFuture<[S3Path]> {
        guard let currentFolder = currentFolder else { return makeFailedFuture(S3FileSystemError.invalidAction) }
        let delimiter: String? = includeSubFolders ? nil : "/"
        var keys: [S3Path] = []
        let request = S3.ListObjectsV2Request(bucket: currentFolder.bucket, delimiter: delimiter, prefix: currentFolder.path.removingPrefix("/"))
        return s3.listObjectsV2Paginator(request) { response, eventLoop in
            keys.append(contentsOf: collate(response))
            return eventLoop.makeSucceededFuture(true)
        }
        .map { return keys }
        .flatMapErrorThrowing { error in
            throw self.convertS3Errors(error)
        }
    }
    

    func makeFailedFuture<T>(_ error: Error) -> EventLoopFuture<T> {
        return s3.client.eventLoopGroup.next().makeFailedFuture(error)
    }
}

