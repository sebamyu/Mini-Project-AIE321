import pandas as pd
from sqlalchemy import create_engine, text

# ==========================================
# 1. การตั้งค่า (Configuration)
# ==========================================
DB_USER = 'postgres'
DB_PASSWORD = 'password'   # รหัสผ่านเดียวกับ ingest.py
DB_HOST = '127.0.0.1'
DB_PORT = '5432'
DB_NAME = 'postgres'

# ชื่อตาราง
SOURCE_TABLE = 'hotel_bookings'
TARGET_TABLE_MAIN = 'cleaned_hotel_bookings'  # ตารางหลักสำหรับทำ Dashboard ละเอียด
TARGET_TABLE_AGG = 'monthly_summary'          # ตารางสรุปยอด (Aggregation)

def main():
    # เชื่อมต่อฐานข้อมูล
    connection_str = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    engine = create_engine(connection_str)
    
    print("🔨 เริ่มต้นกระบวนการ Transform Data...")

    # ---------------------------------------------------------
    # Step 1: สร้าง Schema 'production' (ถ้ายังไม่มี)
    # ---------------------------------------------------------
    try:
        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS production;"))
            conn.commit()
            print("✅ ตรวจสอบ/สร้าง Schema 'production' เรียบร้อย")
    except Exception as e:
        print(f"❌ สร้าง Schema ไม่สำเร็จ: {e}")
        return

    # ---------------------------------------------------------
    # Step 2: อ่านข้อมูลจาก Raw Data
    # ---------------------------------------------------------
    try:
        print(f"📖 กำลังอ่านข้อมูลจาก 'raw_data.{SOURCE_TABLE}'...")
        # อ่านข้อมูลทั้งหมดมาเป็น DataFrame
        df = pd.read_sql(f"SELECT * FROM raw_data.{SOURCE_TABLE}", engine)
        print(f"   - พบข้อมูลดิบ {len(df)} แถว")
    except Exception as e:
        print(f"❌ อ่านข้อมูลไม่สำเร็จ: {e}")
        return

    # ---------------------------------------------------------
    # Step 3: Cleaning & Transformation (แปลงโฉมข้อมูล)
    # ---------------------------------------------------------
    print("⚙️ กำลังทำความสะอาดและสร้างคอลัมน์ใหม่...")

    # 3.1 Cleaning: จัดการ Missing Values (ค่าที่หายไป)
    # ใน dataset นี้ columns: children, country, agent, company มักจะมีค่าว่าง
    df['children'] = df['children'].fillna(0)  # เด็กไม่มีค่า = 0 คน
    df['agent'] = df['agent'].fillna(0)        # ไม่มี agent = 0
    df['company'] = df['company'].fillna(0)    # ไม่มีบริษัท = 0
    df['country'] = df['country'].fillna('Unknown') # ไม่ระบุประเทศ

    # 3.2 Feature Engineering: สร้างคอลัมน์ใหม่ที่มีประโยชน์
    
    # [A] รวมจำนวนคนเข้าพัก (Total Guests)
    df['total_guests'] = df['adults'] + df['children'] + df['babies']

    # [B] รวมจำนวนคืนที่พัก (Total Nights)
    df['total_nights'] = df['stays_in_weekend_nights'] + df['stays_in_week_nights']

    # [C] สร้างวันที่เข้าพักเต็มรูปแบบ (Arrival Date Full)
    # ข้อมูลเดิมแยกเป็น ปี, เดือน(ตัวหนังสือ), วัน -> เอามารวมกันให้เป็น Date จริงๆ
    # เพื่อให้ Looker Studio ใช้เป็น Time Series ได้ง่าย
    month_map = {
        'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6,
        'July': 7, 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12
    }
    df['arrival_date_month_num'] = df['arrival_date_month'].map(month_map)
    
    # สร้างคอลัมน์วันที่ (string format: YYYY-MM-DD)
    df['arrival_full_date'] = pd.to_datetime(dict(year=df.arrival_date_year, 
                                                  month=df.arrival_date_month_num, 
                                                  day=df.arrival_date_day_of_month))

    # [D] คำนวณรายได้ประมาณการ (Estimated Revenue) = ราคาต่อคืน (ADR) * จำนวนคืน
    df['estimated_revenue'] = df['adr'] * df['total_nights']

    print("   - Cleaning & Transformation เสร็จสมบูรณ์!")

    # ---------------------------------------------------------
    # Step 4: Aggregation (สรุปข้อมูลรายเดือน) - โจทย์ข้อ o Aggregation
    # ---------------------------------------------------------
    print("📊 กำลังสร้างตารางสรุปยอดรายเดือน...")
    
    # Group by ปี+เดือน แล้วนับจำนวน Booking และรวมรายได้
    monthly_agg = df.groupby(['arrival_date_year', 'arrival_date_month_num', 'arrival_date_month']) \
                    .agg(
                        total_bookings=('hotel', 'count'),           # นับจำนวนแถว
                        total_revenue=('estimated_revenue', 'sum'),  # ผลรวมรายได้
                        avg_adr=('adr', 'mean')                      # ราคาเฉลี่ย
                    ).reset_index()
    
    # เรียงลำดับตามเวลา
    monthly_agg = monthly_agg.sort_values(by=['arrival_date_year', 'arrival_date_month_num'])

    # ---------------------------------------------------------
    # Step 5: Load to Production (บันทึกผลลัพธ์)
    # ---------------------------------------------------------
    try:
        # 5.1 บันทึกตารางหลัก (Detailed Data)
        print(f"💾 กำลังบันทึกตารางหลักลง 'production.{TARGET_TABLE_MAIN}'...")
        df.to_sql(name=TARGET_TABLE_MAIN, con=engine, schema='production', if_exists='replace', index=False)
        
        # 5.2 บันทึกตารางสรุป (Aggregated Data)
        print(f"💾 กำลังบันทึกตารางสรุปยอดลง 'production.{TARGET_TABLE_AGG}'...")
        monthly_agg.to_sql(name=TARGET_TABLE_AGG, con=engine, schema='production', if_exists='replace', index=False)
        
        print("🎉 เยี่ยมมาก! ข้อมูลพร้อมใช้งานใน Schema 'production' แล้ว")
        
    except Exception as e:
        print(f"❌ บันทึกข้อมูลไม่สำเร็จ: {e}")

if __name__ == "__main__":
    main()