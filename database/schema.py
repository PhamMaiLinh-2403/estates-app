BDS_RAW_LISTINGS_TABLE = """
CREATE TABLE IF NOT EXISTS bds_raw_listings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    listing_id TEXT NOT NULL,
    url TEXT NOT NULL,
    title TEXT,
    short_address TEXT,
    address_parts TEXT,
    latitude REAL,
    longitude REAL,
    main_info TEXT,
    description TEXT,
    other_info TEXT,
    image_urls TEXT,
    content_hash TEXT NOT NULL,
    status TEXT DEFAULT 'NEW',
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(url, content_hash)
)
"""

ONEHOUSING_RAW_LISTINGS_TABLE = """
CREATE TABLE IF NOT EXISTS onehousing_raw_listings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_id TEXT,
    property_url TEXT NOT NULL,
    listing_title TEXT,
    total_price TEXT,
    unit_price TEXT,
    city TEXT,
    district TEXT,
    alley_width TEXT,
    features TEXT, -- Stored as JSON
    property_description TEXT,
    image_url TEXT,
    content_hash TEXT NOT NULL,
    status TEXT DEFAULT 'NEW',
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(property_url, content_hash)
)
"""

CLEANED_LISTINGS_TABLE = """
CREATE TABLE IF NOT EXISTS cleaned_listings (
    ID INTEGER PRIMARY KEY AUTOINCREMENT,
    "Tỉnh/Thành phố" TEXT,
    "Thành phố/Quận/Huyện/Thị xã" TEXT,
    "Xã/Phường/Thị trấn" TEXT,
    "Đường phố" TEXT,
    "Chi tiết" TEXT,
    "Nguồn thông tin" TEXT,
    "Tình trạng giao dịch" TEXT,
    "Thời điểm giao dịch/rao bán" DATE,
    "Thông tin liên hệ" TEXT,
    "Giá rao bán/giao dịch" INTEGER,
    "Giá ước tính" INTEGER,
    "Loại đơn giá (đ/m2 hoặc đ/m ngang)" TEXT,
    "Đơn giá đất" REAL,
    "Lợi thế kinh doanh" TEXT,
    "Số tầng công trình" REAL,
    "Tổng diện tích sàn" REAL,
    "Đơn giá xây dựng" REAL,
    "Năm xây dựng" INTEGER,
    "Chất lượng còn lại" REAL,
    "Diện tích đất (m2)" REAL,
    "Kích thước mặt tiền (m)" REAL,
    "Kích thước chiều dài (m)" REAL,
    "Số mặt tiền tiếp giáp" INTEGER,
    "Hình dạng" TEXT,
    "Độ rộng ngõ/ngách nhỏ nhất (m)" REAL,
    "Khoảng cách tới trục đường chính (m)" REAL,
    "Mục đích sử dụng đất" TEXT,
    "Yếu tố khác" TEXT,
    "Tọa độ (vĩ độ)" REAL,
    "Tọa độ (kinh độ)" REAL,
    "Hình ảnh của bài đăng" TEXT,
)
"""

METADATA_TABLE = """
CREATE TABLE IF NOT EXISTS scraping_metadata (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    websites TEXT NOT NULL,
    scraped_time TIMESTAMP NOT NULL,
    status TEXT NOT NULL,
    is_changed TEXT, 
    error_message TEXT,
)
"""

INDICES = [
    "CREATE INDEX IF NOT EXISTS idx_raw_url ON bds_raw_listings(url)",
    "CREATE INDEX IF NOT EXISTS idx_sec_url ON onehousing_raw_listings(property_url)",
    "CREATE INDEX IF NOT EXISTS idx_sec_prop_id ON onehousing_raw_listings(property_id)",
]