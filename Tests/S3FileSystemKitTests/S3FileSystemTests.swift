import XCTest
import Foundation
import NIO
import S3
@testable import S3FileSystemKit

final class S3FileSystemTests: XCTestCase {
    var s3: S3!
    var s3fs: S3FileSystem!
    
    class TestData {
        let s3fs: S3FileSystem
        let bucket: String
        
        init(_ testName: String, _ s3fs: S3FileSystem) throws {
            self.s3fs = s3fs
            self.bucket = "s3fs-\(testName.lowercased().filter { return $0.isLetter })"
            try s3fs.createBucket(bucketName: bucket)
                .flatMapErrorThrowing { _ in return }
                .flatMap { _ in s3fs.setCurrentFolder(S3Folder(url: "s3://\(self.bucket)")!) }
                .wait()
        }
        
        deinit {
            try? s3fs.setCurrentFolder("/")
            let fileDeletion: EventLoopFuture<Void> = s3fs.listFiles(includeSubFolders: true)
                .flatMap { files in
                    let deleteFutures = files.map { self.s3fs.deleteFile($0.file) }
                    return EventLoopFuture.whenAllComplete(deleteFutures, on: self.s3fs.s3.client.eventLoopGroup.next()).map { _ in return }
            }
            try? fileDeletion.wait()
            try? s3fs.deleteBucket(bucketName: bucket).wait()
        }
    }
    
    override func setUp() {
        s3 = S3(
            accessKeyId: "key",
            secretAccessKey: "secret",
            region: .euwest1,
            endpoint: ProcessInfo.processInfo.environment["S3_ENDPOINT"] ?? "http://localhost:4572"
        )
        //s3 = S3(region: .euwest1)
        s3fs = S3FileSystem(s3)
    }

    func testS3Folder() {
        let folder1 = S3Folder(url: "s3://my-bucket")
        let folder2 = S3Folder(url: "s3://my-bucket/")
        let folder3 = S3Folder(url: "S3://my-bucket/folder")
        let folder4 = S3Folder(url: "s3://my-bucket/folder/")
        let folder5 = S3Folder(url: "S3://my-bucket/folder/folder2")
        let folder6 = S3Folder(url: "S4://my-bucket/folder/folder2")

        XCTAssertEqual(folder1?.bucket, "my-bucket")
        XCTAssertEqual(folder1?.path, "")
        XCTAssertEqual(folder2?.bucket, "my-bucket")
        XCTAssertEqual(folder2?.path, "")
        XCTAssertEqual(folder3?.bucket, "my-bucket")
        XCTAssertEqual(folder3?.path, "folder/")
        XCTAssertEqual(folder4?.bucket, "my-bucket")
        XCTAssertEqual(folder4?.path, "folder/")
        XCTAssertEqual(folder5?.bucket, "my-bucket")
        XCTAssertEqual(folder5?.path, "folder/folder2/")
        XCTAssertNil(folder6)
    }

    func testS3File() {
        let file1 = S3File(url: "s3://my-bucket/file")
        let file2 = S3File(url: "S3://my-bucket/folder/file")
        let file3 = S3File(url: "s3://my-bucket/file/")

        XCTAssertEqual(file1?.bucket, "my-bucket")
        XCTAssertEqual(file1?.path, "file")
        XCTAssertEqual(file2?.bucket, "my-bucket")
        XCTAssertEqual(file2?.path, "folder/file")
        XCTAssertNil(file3)
    }

    func testURL() {
        let file = S3File(url: "s3://bucket/folder/file")
        XCTAssertEqual(file?.url, "s3://bucket/folder/file")
    }
    
    func testSubFolder() {
        let folder = S3Folder(url: "s3://bucket/folder")
        let subfolder = folder?.subFolder("folder2")
        XCTAssertEqual(subfolder?.url, "s3://bucket/folder/folder2/")
    }
    
    func testFileInFolder() {
        let folder = S3Folder(url: "s3://bucket/folder")
        let file = folder?.file("file")
        XCTAssertEqual(file?.url, "s3://bucket/folder/file")
    }
    
    func testFileNameExtension() {
        let file = S3File(url: "s3://bucket/folder/file.txt")
        let name = file?.name
        let nameWithoutExtension = file?.nameWithoutExtension
        let `extension` = file?.extension
        
        XCTAssertEqual(name, "file.txt")
        XCTAssertEqual(nameWithoutExtension, "file")
        XCTAssertEqual(`extension`, "txt")
    }
    
    func testPushPopFolder() {
        do {
            let testData = try TestData(#function, s3fs)
            _ = try s3fs.setCurrentFolder(S3Folder(url: "s3://\(testData.bucket)")!).wait()
            try s3fs.pushFolder("folder1")
            try s3fs.popFolder()
            try s3fs.pushFolder("folder2")
            try s3fs.popFolder()
        } catch {
            XCTFail("\(error)")
        }
    }
    
    func testBucketCreateExistsDelete() {
        do {
            let bucketName = "s3fs-\(#function.lowercased().filter { return $0.isLetter })"
            let future: EventLoopFuture<Bool> = s3fs.createBucket(bucketName: bucketName)
                .flatMap { _ in
                    return self.s3fs.doesBucketExist(bucketName: bucketName)
                }.flatMap { exists in
                    XCTAssertEqual(exists, true)
                    return self.s3fs.deleteBucket(bucketName: bucketName)
                }.flatMap { () in
                    return self.s3fs.doesBucketExist(bucketName: bucketName)
            }
            let exists = try future.wait()
            XCTAssertEqual(exists, false)
        } catch {
            XCTFail("\(error)")
        }
    }

    func testBucketDoesntExist() {
        do {
            _ = try s3fs.setCurrentFolder(S3Folder(url: "s3://s3fs-nonexistentbucket")!).wait()
            XCTFail("Shouldn't get here")
        } catch S3FileSystemError.bucketDoesNotExist {
        } catch {
            XCTFail("\(error)")
        }
    }
    
    func testBucketDoesntExist2() {
        do {
            _ = try s3fs.readFile(S3File(url: "s3://s3fs-nonexistentbucket/test")!).wait()
            XCTFail("Shouldn't get here")
        } catch S3FileSystemError.bucketDoesNotExist {
        } catch {
            XCTFail("\(error)")
        }
    }
    
    func testListBuckets() {
        do {
            let testData = try TestData(#function, s3fs)
            let buckets = try s3fs.listBuckets().wait()
            XCTAssertNotNil(buckets.first { $0 == S3Folder(url: "s3://\(testData.bucket)/")!})
        } catch {
            XCTFail("\(error)")
        }
    }
    
    func testWriteReadFile() {
        do {
            let testData = try TestData(#function, s3fs)
            let data = Data("My test data".utf8)
            let future = s3fs.setCurrentFolder(S3Folder(url: "s3://\(testData.bucket)/folder")!)
                .flatMap { _ in
                    return self.s3fs.writeFile(name: "testfile", data: data)
                }.flatMap { _ in
                    return self.s3fs.readFile(name: "testfile")
            }
            let result = try future.wait()
            XCTAssertEqual(result, data)
            try s3fs.deleteFile(name: "testfile").wait()
        } catch {
            XCTFail("\(error)")
        }
    }
    
    func testFileSize() {
        do {
            let testData = try TestData(#function, s3fs)
            let data = Data("My test data".utf8)
            let future = s3fs.setCurrentFolder(S3Folder(url: "s3://\(testData.bucket)/folder")!)
                .flatMap { _ in
                    return self.s3fs.writeFile(name: "testfile", data: data)
                }.flatMap { _ in
                    return self.s3fs.getFileAttributes(name: "testfile")
            }
            let result = try future.wait()
            XCTAssertEqual(result.size, 12)
        } catch {
            XCTFail("\(error)")
        }
    }
    
    func testFileAttributes() {
        do {
            let testData = try TestData(#function, s3fs)
            let data = Data("My test data".utf8)
            let future = s3fs.setCurrentFolder(S3Folder(url: "s3://\(testData.bucket)/folder")!)
                .flatMap { _ in
                    return self.s3fs.writeFile(name: "testfile", data: data, attributes: .init(contentType: "text/plain"))
                }.flatMap { _ in
                    return self.s3fs.getFileAttributes(name: "testfile")
            }
            let result = try future.wait()
            XCTAssertEqual(result.contentType, "text/plain")
        } catch {
            XCTFail("\(error)")
        }
    }
    
    func createFiles(name: String, number: Int) -> EventLoopFuture<Void> {
        var responses : [EventLoopFuture<Void>] = []
        for i in 0..<number {
            let objectName = "\(name)\(i)"
            let data = Data("Test\(i)".utf8)
            let response = s3fs.writeFile(name: objectName, data: data)
            responses.append(response)
        }

        return EventLoopFuture.whenAllComplete(responses, on: s3.client.eventLoopGroup.next()).map { _ in return }
    }
    
    func testListFiles() {
        do {
            let testData = try TestData(#function, s3fs)
            
            try s3fs.setCurrentFolder(S3Folder(url: "s3://\(testData.bucket)")!).wait()
            _ = try createFiles(name: "test-", number: 6).wait()
            try s3fs.pushFolder("folder")
            _ = try createFiles(name: "test2-", number: 5).wait()
            try s3fs.popFolder()
            try s3fs.pushFolder("folder2")
            _ = try createFiles(name: "test3-", number: 2).wait()
            try s3fs.popFolder()

            let list = try s3fs.listFiles().wait()
            XCTAssertEqual(list.count, 6)
            
            let folders = try s3fs.listSubfolders().wait()
            XCTAssertEqual(folders.count, 2)
            
            try s3fs.pushFolder("folder")
            let list2 = try s3fs.listFiles().wait()
            XCTAssertEqual(list2.count, 5)

            try s3fs.popFolder()
            let list3 = try s3fs.listFiles(includeSubFolders: true).wait()
            XCTAssertEqual(list3.count, 13)

        } catch {
            XCTFail("\(error)")
        }
    }
    
    func testCopyFiles() {
        do {
            let testData = try TestData(#function, s3fs)

            let from = S3File(url: "s3://\(testData.bucket)/folder1/testObject.txt")!
            let to = S3File(url: "s3://\(testData.bucket)/folder2/testObject.txt")!
            
            let data = Data("Test string".utf8)
            try s3fs.writeFile(from, data: data).wait()
            try s3fs.copyFile(from: from, to: to).wait()
            let data2 = try s3fs.readFile(to).wait()
            
            XCTAssertEqual(data, data2)
        } catch {
            XCTFail("\(error)")
        }
    }
    
    static var allTests = [
        ("testS3Folder", testS3Folder),
        ("testS3File", testS3File),
        ("testURL", testURL),
        ("testSubFolder", testSubFolder),
        ("testFileInFolder", testFileInFolder),
        ("testFileNameExtension", testFileNameExtension),
        ("testPushPopFolder", testPushPopFolder),
        ("testBucketCreateExistsDelete", testBucketCreateExistsDelete),
        ("testBucketDoesntExist", testBucketDoesntExist),
        ("testBucketDoesntExist2", testBucketDoesntExist2),
        ("testListBuckets", testListBuckets),
        ("testWriteReadFile", testWriteReadFile),
        ("testFileSize", testFileSize),
        ("testFileAttributes", testFileAttributes),
        ("testListFiles", testListFiles),
        ("testCopyFiles", testCopyFiles),
    ]
}
