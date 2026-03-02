import re
import pandas as pd
import numpy as np

from commons.config import *


class OneHousingDataCleaner:
    """
    Cleans and transforms raw OneHousing scraped data into standardized format.
    """

    @staticmethod
    def _extract_city(row):
        """Extract and standardize city name."""
        city = row.get('city')

        if pd.notna(city):
           return str(row["city"]).replace("TP.", "Thành phố").replace('T.', 'Tỉnh').strip()
        
        else:
            city = row.get('listing_title', '')
            try:
                return city.split(',')[-1].replace('TP.', 'Thành phố').replace('T.', 'Tỉnh').strip()
            except:
                return np.nan

    @staticmethod
    def _extract_district(row):
        """Extract and standardize district name."""
        district = row.get("district")

        if pd.notna(district):
            return str(district).replace("Q.", "Quận").replace("H.", "Huyện").replace("TX.", "Thị xã").strip()

        title = row.get('listing_title', '')

        if pd.notna(title):
            return title.split(",")[-2].replace('TP.', "Thành phố").replace('Q.', "Quận").replace('H.', "Huyện").replace('TX.', 'Thị xã').strip()
        
        return np.nan

    @staticmethod
    def _extract_ward(df):
        """Extract ward/commune information."""
        def extract_row(row):
            full_address = row.get('listing_title', '')
            district = row.get('district', '')

            if pd.isna(full_address) or not isinstance(full_address, str) or \
                    pd.isna(district) or not isinstance(district, str):
                return np.nan

            address_list = full_address.split(",")

            try:
                return address_list[-3].replace("X.", "Xã").replace("P.", "Phường").replace("TT.", "Thị trấn").strip()
            except:
                return np.nan

        return df.apply(extract_row, axis=1)

    @staticmethod
    def _extract_street_name(series: pd.Series) -> pd.Series:
        """Extract street name from listing title."""
        def extract(text: str):
            if pd.isna(text):
                return np.nan

            patterns = [
                r"(?:Nhà mặt ngõ|Đất nền|Nhà trong ngõ).*?cách\s+(.*?)\s*\d+(?:\.\d+)?m",
                r"(?:Nhà mặt phố|Mặt đường)\s+([^,]+?)\s*,",
                r"Đất nền\s+((?!.*cách)[^,]+?)\s*,"
            ]

            for p in patterns:
                if match := re.search(p, str(text), re.IGNORECASE):
                    return re.sub(r'\s*\(.*\)\s*$', '', match.group(1).strip()).strip()
                
            return np.nan

        return series.apply(extract)

    @staticmethod
    def _classify_property_type(title: str) -> str:
        """Classify property as street-front or alley."""
        if pd.isna(title):
            return ""

        if "cách" in str(title).lower() or "mặt ngõ" in str(title).lower():
            return "Mặt ngõ"

        return "Mặt phố"

    @staticmethod
    def _convert_price_to_numeric(price_str: str) -> float:
        """Convert price string to numeric value."""
        if pd.isna(price_str):
            return np.nan

        price = str(price_str).lower()

        try:
            val_str = price.replace(',', '.').strip()
            if 'tỷ' in val_str:
                return float(val_str.replace('tỷ', '').strip()) * 1e9
            if 'triệu' in val_str:
                return float(val_str.replace('triệu', '').strip()) * 1e6
            return float(val_str)
        
        except (ValueError, AttributeError):
            return np.nan

    @staticmethod
    def _estimate_price(price: float) -> float:
        """Estimate actual price (98% of listed price)."""
        return round(price * 0.98, 2) if pd.notna(price) else np.nan

    @staticmethod
    def _extract_alley_width(row):
        """Extract minimum alley width."""
        for text in [row.get("alley_width"), row.get("property_description")]:
            if pd.notna(text):
                if nums := re.findall(r"(\d+(?:\.\d+)?)", str(text)):

                    try:
                        return min(float(n) for n in nums)
                    except ValueError:
                        continue

        return np.nan

    @staticmethod
    def _extract_front_width(row):
        """Extract front width from features or description."""
        sources = [row.get('features'), row.get('property_description')]
        patterns = [
            r"Hướng mặt tiền\s*:[^;-]+?-\s*(\d+(?:\.\d+)?)\s*m",
            r"Nhà mặt tiền\s+(\d+(?:\.\d+)?)\s*m"
        ]
        
        for text in sources:
            if pd.notna(text):
                for p in patterns:
                    if match := re.search(p, str(text), re.IGNORECASE):
                        try:
                            return float(match.group(1))
                        except ValueError:
                            pass
        return np.nan

    @staticmethod
    def _extract_number_of_floors(row):
        """Extract total number of floors."""
        title = str(row.get("listing_title", "")).lower()
        if "đất nền" in title:
            return 0.0

        text = str(row.get("features", "")) + " " + str(row.get("property_description", ""))
        floor = re.search(r"Số tầng:\s*(\d+(?:\.\d+)?)", text, re.IGNORECASE)
        basement = re.search(r"Số tầng hầm:\s*(\d+(?:\.\d+)?)", text, re.IGNORECASE)
        total_floors = 0.0
        found = False

        if floor:
            total_floors += float(floor.group(1))
            found = True
        if basement:
            total_floors += float(basement.group(1))
            found = True

        return total_floors if found else np.nan

    @staticmethod
    def _extract_land_area(row):
        """Extract land area."""
        text = str(row.get("features", "")) + " " + str(row.get("property_description", ""))
        patterns = [r"Diện tích:\s*(\d+(?:\.\d+)?)", r"diện tích đất thực tế là\s*([\d.]+)m²"]

        for p in patterns:
            if match := re.search(p, text, re.IGNORECASE):
                try:
                    return float(match.group(1))
                except (ValueError, IndexError):
                    pass

        return np.nan

    @staticmethod
    def _extract_distance_to_main_road(row):
        """Extract distance to main road."""
        desc = str(row.get('property_description', ''))
        title = str(row.get('listing_title', ''))

        if 'mặt phố' in title.lower():
            return 0

        patterns = [
            r"khoảng cách ra trục đường chính\s*(\d+(?:\.\d+)?)\s*m",
            r"cách\s+.*?\s+(\d+(?:\.\d+)?)\s*m"
        ]
        text_sources = [desc, title]

        for text, p in zip(text_sources, patterns):
            if match := re.search(p, text, re.IGNORECASE):
                return float(match.group(1))

        return 0

    @staticmethod
    def _extract_number_of_frontages(row):
        """Extract number of frontages."""
        text = row.get("property_description")
        if pd.isna(text):
            return 1

        match = re.search(r"(\d+)\s*mặt tiền", str(text), re.IGNORECASE)
        if match:
            try:
                return int(match.group(1))
            except (ValueError, IndexError):
                pass

        return 1

    @staticmethod
    def _estimate_remaining_quality(row):
        """Estimate remaining quality of construction."""
        title = str(row.get("listing_title", "")).lower()

        if "đất nền" in title:
            return ""
        return 0.85

    @staticmethod
    def _estimate_construction_price(row):
        """Estimate construction price per sqm."""
        text = str(row.get("features", "")) + " " + str(row.get("property_description", ""))
        floor = re.search(r"Số tầng:\s*(\d+(?:\.\d+)?)", text, re.IGNORECASE)
        basement = re.search(r"Số tầng hầm:\s*(\d+(?:\.\d+)?)", text, re.IGNORECASE)
        total_floors = 0.0

        if "đất nền" in text:
            return 0

        total_floors += float(floor.group(1)) if floor else 0.0
        total_floors += float(basement.group(1)) if basement else 0.0

        if total_floors == 1:
            return 6_000_000

        if floor is not None and basement is not None and float(floor.group(1)) > 1 and float(basement.group(1)) > 0:
            return  11_000_000

        if total_floors > 1 and basement is not None and float(basement.group(1)) == 0:
            return 9_500_000
        
        return 4_000_000 
    
    @staticmethod
    def clean_onehousing_data(df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply all cleaning transformations to OneHousing raw data.
        """   
        # Rename property_id to ID for consistency
        if 'property_id' in df.columns:
            df = df.copy()
            df.rename(columns={'property_id': 'ID'}, inplace=True)
        
        # Extract and transform all fields
        city = df.apply(OneHousingDataCleaner._extract_city, axis=1)
        district = df.apply(OneHousingDataCleaner._extract_district, axis=1)
        
        # Temporarily add district column for location extraction
        df_temp = df.copy()
        df_temp['district'] = district
        location = OneHousingDataCleaner._extract_ward(df_temp)
        
        street = OneHousingDataCleaner._extract_street_name(df["listing_title"])
        prop_type = df["listing_title"].apply(OneHousingDataCleaner._classify_property_type)
        price = df["total_price"].apply(OneHousingDataCleaner._convert_price_to_numeric)
        est_price = price.apply(OneHousingDataCleaner._estimate_price)
        floors = df.apply(OneHousingDataCleaner._extract_number_of_floors, axis=1)
        num_frontages = df.apply(OneHousingDataCleaner._extract_number_of_frontages, axis=1)
        area = df.apply(OneHousingDataCleaner._extract_land_area, axis=1)
        front_width = df.apply(OneHousingDataCleaner._extract_front_width, axis=1)
        remaining_quality = df.apply(OneHousingDataCleaner._estimate_remaining_quality, axis=1)
        construction_price = df.apply(OneHousingDataCleaner._estimate_construction_price, axis=1)
        latitude = df["latitude"]
        longitude = df["longitude"]

        with np.errstate(divide='ignore', invalid='ignore'):
            floors_for_calc = floors.fillna(1.0)
            total_area = round((floors_for_calc * area).replace([np.inf, -np.inf], np.nan), 2)
            length = round((area / front_width).replace([np.inf, -np.inf], np.nan), 2)

        cleaned_df = pd.DataFrame({
            "Tỉnh/Thành phố": city,
            "Thành phố/Quận/Huyện/Thị xã": district,
            "Xã/Phường/Thị trấn": location,
            "Đường phố": street,
            "Chi tiết": prop_type,
            "Nguồn thông tin": df["property_url"],
            "Tình trạng giao dịch": "Đang giao dịch",
            "Thời điểm giao dịch/rao bán": np.nan,
            "Thông tin liên hệ": "",
            "Giá rao bán/giao dịch": price,
            "Giá ước tính": est_price,
            "Loại đơn giá (đ/m2 hoặc đ/m ngang)": "đ/m2",
            "Đơn giá đất": "",
            "Lợi thế kinh doanh": "",
            "Số tầng công trình": floors,
            "Tổng diện tích sàn": total_area,
            "Đơn giá xây dựng": construction_price,
            "Năm xây dựng": np.nan,
            "Chất lượng còn lại": remaining_quality,
            "Diện tích đất (m2)": area,
            "Kích thước mặt tiền (m)": front_width,
            "Kích thước chiều dài (m)": length,
            "Số mặt tiền tiếp giáp": num_frontages,
            "Hình dạng": "Chữ nhật",
            "Độ rộng ngõ/ngách nhỏ nhất (m)": df.apply(OneHousingDataCleaner._extract_alley_width, axis=1),
            "Khoảng cách tới trục đường chính (m)": df.apply(OneHousingDataCleaner._extract_distance_to_main_road, axis=1),
            "Mục đích sử dụng đất": "Đất ở",
            "Yếu tố khác": "",
            "Tọa độ (vĩ độ)": latitude,
            "Tọa độ (kinh độ)": longitude,
            "Hình ảnh của bài đăng": df["image_url"]
        })

        # Drop NaN values
        na = [
            'Tỉnh/Thành phố',
            'Thành phố/Quận/Huyện/Thị xã',
            'Xã/Phường/Thị trấn',
            'Đường phố',
            'Chi tiết',
            'Nguồn thông tin', 
            # 'Thời điểm giao dịch/rao bán',
            'Giá rao bán/giao dịch',
            'Giá ước tính',
            # 'Đơn giá đất',
            # 'Lợi thế kinh doanh',
            'Số tầng công trình', 
            'Tổng diện tích sàn', 
            'Đơn giá xây dựng',
            'Chất lượng còn lại',
            'Diện tích đất (m2)',
            'Kích thước mặt tiền (m)',
            'Kích thước chiều dài (m)',
            'Số mặt tiền tiếp giáp',
            'Hình dạng',
            'Độ rộng ngõ/ngách nhỏ nhất (m)',
            'Khoảng cách tới trục đường chính (m)',
            'Mục đích sử dụng đất',
            'Tọa độ (vĩ độ)', 
            'Tọa độ (kinh độ)' 
        ]
        cleaned_df = cleaned_df.dropna(subset=na)

        # Drop duplicates
        dup = [
            'Tỉnh/Thành phố', 
            'Thành phố/Quận/Huyện/Thị xã', 
            'Xã/Phường/Thị trấn', 
            'Đường phố', 
            'Giá rao bán/giao dịch', 
            'Giá ước tính', 
            # 'Đơn giá đất', 
            # 'Lợi thế kinh doanh', 
            'Số tầng công trình', 
            'Tổng diện tích sàn', 
            'Đơn giá xây dựng', 
            'Chất lượng còn lại', 
            'Diện tích đất (m2)', 
            'Kích thước mặt tiền (m)', 
            'Kích thước chiều dài (m)', 
            'Số mặt tiền tiếp giáp', 
            'Hình dạng', 
            'Độ rộng ngõ/ngách nhỏ nhất (m)', 
            'Khoảng cách tới trục đường chính (m)', 
            'Mục đích sử dụng đất'
        ]
        cleaned_df = cleaned_df.drop_duplicates(subset=dup)
        
        print("[OneHousing] Data cleaning completed.")
        return cleaned_df