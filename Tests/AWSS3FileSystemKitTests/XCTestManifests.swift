import XCTest

#if !canImport(ObjectiveC)
public func allTests() -> [XCTestCaseEntry] {
    return [
        testCase(awsS3FileSystemTests.allTests),
    ]
}
#endif
