// swift-tools-version: 6.2
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let swiftSettings: [SwiftSetting] = [
    // Nonisolated async functions run on the caller's executor (SE-0461).
    .enableUpcomingFeature("NonisolatedNonsendingByDefault"),
]

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
            swiftSettings: swiftSettings
        ),
        .testTarget(
            name: "MQTTTests",
            dependencies: ["MQTT"],
            swiftSettings: swiftSettings
        ),
    ]
)
