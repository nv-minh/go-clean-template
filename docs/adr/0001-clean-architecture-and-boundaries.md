# ADR 0001: Clean Architecture, ranh giới được cưỡng chế bằng lint

Trạng thái: Accepted

## Bối cảnh

Dự án sống nhiều năm và nhiều người cùng sửa.
Kiến trúc chỉ tồn tại nếu ranh giới giữa các lớp được bảo vệ tự động, không dựa vào việc nhớ hay review bằng mắt.

## Quyết định

Chia thành `domain`, `usecase`, `adapter`, `platform` và `cmd` với quy tắc: phụ thuộc chỉ hướng vào trong.
Port (interface) được khai báo ở `domain`, adapter cài đặt chúng.
Quy tắc được kiểm tra bởi `depguard` trong `.golangci.yml`, nên vi phạm làm CI thất bại.

## Các lựa chọn đã cân nhắc

| Lựa chọn | Đánh giá |
|----------|----------|
| Tổ chức theo loại (`controllers/`, `services/`, `models/`) | Đơn giản nhưng không có ranh giới thật, dễ thành "mớ bòng bong" khi lớn lên |
| Kiến trúc nhiều lớp đầy đủ kiểu Java (nhiều interface, mapper cho từng lớp) | Quá nhiều nghi lễ, không hợp với Go |
| Clean Architecture gọn (chọn) | Đủ ranh giới để test và thay thế adapter, ít file thừa |

## Hệ quả

- Unit test usecase chạy bằng fake trong bộ nhớ, không cần Docker.
- Thêm adapter mới (ví dụ replica đọc) không động tới usecase.
- Phải viết thêm interface và mapping giữa DTO, domain, hàng DB. Đó là chi phí chấp nhận được để đổi lấy ranh giới.
- Quy tắc YAGNI: chỉ tạo interface khi có từ hai bên dùng chung hoặc cần fake để test, không tạo trước.
