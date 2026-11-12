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
    targets: [
        .target(
            name: "MQTT",
            swiftSettings: swiftSettings
        ),
        .testTarget(
            name: "MQTTTests",
            dependencies: ["MQTT"],
            swiftSettings: swiftSettings
        ),
    ]
)
