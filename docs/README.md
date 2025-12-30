# Tài Liệu Dự Án

Thư mục này chứa tất cả tài liệu quan trọng cho dự án Pipeline Phân Tích Giá E-commerce.

## 📋 Danh Sách Tài Liệu

### 1. [task.md](task.md) - Danh Sách Công Việc
**Mục đích**: Theo dõi tiến độ triển khai  
**Nội dung**:
- 12 phases chi tiết
- Checklist từng công việc
- Trạng thái hoàn thành (✅/⏳/❌)

**Cách sử dụng**: Cập nhật khi hoàn thành mỗi task

---

### 2. [implementation_plan.md](implementation_plan.md) - Kế Hoạch Triển Khai
**Mục đích**: Kế hoạch tổng thể và chi tiết kỹ thuật  
**Nội dung**:
- Gap analysis (so sánh hiện tại vs yêu cầu)
- 9 phases triển khai
- Timeline 23 ngày
- Verification plan
- User review requirements

**Cách sử dụng**: Tham khảo trước khi bắt đầu mỗi phase

---

### 3. [walkthrough_phase1_2.md](walkthrough_phase1_2.md) - Hướng Dẫn Phase 1 & 2
**Mục đích**: Ghi chép chi tiết những gì đã làm  
**Nội dung**:
- Phase 1: Data Sources & Schema Design
- Phase 2: Streaming Infrastructure Setup
- Code examples
- Testing procedures
- Verification steps

**Cách sử dụng**: Đọc để hiểu cách hoạt động của Phase 1 & 2

---

### 4. [data_sources.md](data_sources.md) - Nguồn Dữ Liệu
**Mục đích**: Tài liệu về nguồn dữ liệu và schema  
**Nội dung**:
- Websites được chọn (webscraper.io, Tiki, Shopee, Sendo)
- Schema design (raw, clean, alerts)
- 5 data challenges + solutions
- Code examples cho xử lý dữ liệu

**Cách sử dụng**: Tham khảo khi làm việc với dữ liệu

---

## 📊 Mối Quan Hệ Giữa Các Tài Liệu

```
implementation_plan.md (Kế hoạch tổng thể)
        │
        ├─▶ task.md (Checklist chi tiết)
        │
        ├─▶ data_sources.md (Spec nguồn dữ liệu)
        │
        └─▶ walkthrough_phase1_2.md (Ghi chép đã làm)
                │
                └─▶ (Sẽ có walkthrough_phase3.md, phase4.md...)
```

## 🔄 Quy Trình Làm Việc

### Khi Bắt Đầu Phase Mới:
1. Đọc **implementation_plan.md** để hiểu yêu cầu
2. Cập nhật **task.md** đánh dấu tasks đang làm
3. Tham khảo **data_sources.md** nếu cần

### Khi Hoàn Thành Phase:
1. Cập nhật **task.md** đánh dấu hoàn thành
2. Tạo **walkthrough_phaseX.md** ghi chép chi tiết
3. Cập nhật **README.md** chính

## 📝 Template Walkthrough

Khi tạo walkthrough cho phase mới, bao gồm:

```markdown
# Hướng Dẫn: Phase X - [Tên Phase]

## Tổng Quan
[Mô tả ngắn gọn]

## Những Gì Đã Xây Dựng
[Chi tiết từng component]

## Kiểm Tra & Xác Minh
[Các bước test]

## Cấu Trúc File
[Danh sách files đã tạo]

## Thành Tựu Chính
[Checklist hoàn thành]

## Bước Tiếp Theo
[Phase tiếp theo]
```

## 🎯 Mục Tiêu Tài Liệu

- ✅ **Rõ ràng**: Dễ hiểu cho người đọc
- ✅ **Đầy đủ**: Bao gồm tất cả thông tin cần thiết
- ✅ **Cập nhật**: Phản ánh đúng trạng thái hiện tại
- ✅ **Tiếng Việt**: Dễ tiếp cận cho team

## 📌 Lưu Ý Quan Trọng

> **⚠️ Các file artifact chỉ tồn tại trong conversation**  
> Các file trong `C:\Users\Admin\.gemini\antigravity\brain\...` sẽ mất khi conversation kết thúc.  
> **Đã copy sang `docs/` để lưu trữ vĩnh viễn!**

## 🔗 Tài Liệu Khác

- **[../kafka/README.md](../kafka/README.md)** - Hướng dẫn Kafka Streaming
- **[../README.md](../README.md)** - README chính của dự án

---

**Cập nhật**: 2025-12-30  
**Trạng thái**: Phase 2/12 hoàn thành
