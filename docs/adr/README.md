# Architecture Decision Records

Mỗi ADR ghi lại một quyết định, bối cảnh, các lựa chọn đã cân nhắc và hệ quả.
Khi muốn đổi một quyết định, thêm ADR mới thay thế (đặt trạng thái "Superseded") thay vì sửa lịch sử.

| ADR | Quyết định |
|-----|------------|
| [0001](0001-clean-architecture-and-boundaries.md) | Clean Architecture, quy tắc phụ thuộc được cưỡng chế bằng lint |
| [0002](0002-transactional-outbox-with-kafka.md) | Transactional Outbox và Kafka cho sự kiện |
| [0003](0003-chi-pgx-sqlc.md) | `chi`, `pgx`, `sqlc` thay vì Fiber, GORM |
| [0004](0004-keyset-pagination-and-uuidv7.md) | Phân trang keyset và UUIDv7 |
| [0005](0005-redis-usage-and-failure-policy.md) | Cách dùng Redis và chính sách khi Redis lỗi |
| [0006](0006-idempotency-keys.md) | Idempotency bằng header `Idempotency-Key` |
