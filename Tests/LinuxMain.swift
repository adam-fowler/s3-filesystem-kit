import XCTest

import aws_s3_fsTests

var tests = [XCTestCaseEntry]()
tests += aws_s3_fsTests.allTests()
XCTMain(tests)
