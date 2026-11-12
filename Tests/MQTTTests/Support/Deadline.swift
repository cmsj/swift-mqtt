import Foundation
import Synchronization

/// Resumes a continuation at most once, from whichever side gets there first.
private final class OnceContinuation<T: Sendable>: Sendable {
    private let slot = Mutex<CheckedContinuation<T, any Error>?>(nil)

    func install(_ continuation: CheckedContinuation<T, any Error>) {
        slot.withLock { $0 = continuation }
    }

    func resume(with result: Result<T, any Error>) {
        slot.withLock { $0.take() }?.resume(with: result)
    }
}

/// Runs `operation` but gives up after `timeout`, even if the operation ignores
/// cancellation. The legacy client's `Promise.wait()` never observes cancellation,
/// so without this a request the client loses track of hangs the whole test run.
func withDeadline<T: Sendable>(
    _ what: String,
    within timeout: Duration = .prompt,
    _ operation: @escaping @Sendable () async throws -> T
) async throws -> T {
    let once = OnceContinuation<T>()
    return try await withCheckedThrowingContinuation { continuation in
        // Install before starting the work, so a fast result can't be dropped.
        once.install(continuation)
        let work = Task {
            do {
                once.resume(with: .success(try await operation()))
            } catch {
                once.resume(with: .failure(error))
            }
        }
        Task {
            try? await Task.sleep(for: timeout)
            work.cancel()
            once.resume(with: .failure(MailboxError.timedOut(what)))
        }
    }
}
