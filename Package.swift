// swift-tools-version: 6.2
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "swift-mqtt",
    platforms: [.macOS("27.0")],
    products: [
        .library(
            name: "MQTT",
            targets: ["MQTT"]),
    ],
    dependencies: [
        .package(url: "https://github.com/sutext/swift-promise", from: "2.3.0"),
    ],
    targets: [
        .target(
            name: "MQTT",
            dependencies: [
                .product(name: "Promise", package: "swift-promise")
            ],
            // The legacy client is not Swift 6 clean; it is replaced by the actor rewrite.
            swiftSettings: [.swiftLanguageMode(.v5)]
        ),
        .testTarget(
            name: "MQTTTests",
            dependencies: ["MQTT"]),
    ]
)
