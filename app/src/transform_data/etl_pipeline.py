import pandas as pd
import uuid
import re

# CÁC HÀM BÓC TÁCH & CHUẨN HÓA DỮ LIỆU
def extract_form(text):
    """Bóc tách Dạng bào chế từ chuỗi Description"""
    form_patterns = r'(Inhaler|Tablet|Capsule|Injection|Syrup|Cream|Ointment|Liquid|Drops|Lotion|Solution|Suspension|Syringe|Auto-Injector)'
    match = re.search(form_patterns, str(text), re.IGNORECASE)
    return match.group(0).title() if match else 'Unknown'

def extract_strength(text):
    """Bóc tách Hàm lượng từ chuỗi Description"""
    strength_pattern = r'(\d+(?:\.\d+)?\s*(?:MG|ML|UNT|MCG|%|HR)[a-zA-Z/]*)'
    matches = re.findall(strength_pattern, str(text), re.IGNORECASE)
    return ' / '.join(matches) if matches else 'Unknown'

def predict_drug_class(text):
    """Dự đoán Nhóm thuốc dựa trên tên hoạt chất (Từ điển thuần tiếng Anh)"""
    text = str(text).lower()
    
    class_dict = {
        'Analgesic': [
            'acetaminophen', 'hydrocodone', 'oxycodone', 'ibuprofen', 'naproxen', 
            'fentanyl', 'tramadol', 'meperidine', 'morphine', 'alfentanil', 'phenazopyridine', 'aspirin'
        ],
        'Antibiotic': [
            'amoxicillin', 'penicillin', 'azithromycin', 'cephalexin', 'ciprofloxacin', 
            'piperacillin', 'tazobactam', 'vancomycin', 'doxycycline', 'nitrofurantoin', 'clavulanate'
        ],
        'Antihypertensive': [
            'lisinopril', 'amlodipine', 'losartan', 'metoprolol', 'hydrochlorothiazide',
            'carvedilol', 'verapamil', 'captopril', 'valsartan', 'sacubitril'
        ],
        'Cardiovascular': [
            'clopidogrel', 'digoxin', 'amiodarone', 'nitroglycerin', 'alteplase'
        ],
        'Antidiabetic': [
            'metformin', 'insulin', 'glipizide'
        ],
        'Antihyperlipidemic': [
            'atorvastatin', 'simvastatin', 'rosuvastatin'
        ],
        'Antidepressant': [
            'sertraline', 'fluoxetine', 'escitalopram', 'citalopram'
        ],
        'Asthma/COPD': [
            'albuterol', 'fluticasone', 'salmeterol', 'budesonide'
        ],
        'Anticoagulant': [
            'warfarin', 'rivaroxaban', 'apixaban', 'enoxaparin', 'heparin'
        ],
        'Antihistamine': [
            'loratadine', 'terfenadine', 'astemizole', 'chlorpheniramine', 'diphenhydramine', 'epinephrine'
        ],
        'Contraceptive': [
            'mestranol', 'norethynodrel', 'natazia', 'trinessa', 'levora', 'seasonique', 
            'errin', 'camila', 'jolivette', 'yaz', 'norinyl', 'estrostep', 
            'medroxyprogesterone', 'etonogestrel', 'levonorgestrel', 'mirena', 'liletta', 'kyleena'
        ],
        'Antineoplastic': [
            'docetaxel', 'cisplatin', 'paclitaxel', 'etoposide', 'letrozole', 
            'verzenio', 'trastuzumab', 'palbociclib', 'tamoxifen', 'leuprolide'
        ],
        'Dementia': [
            'galantamine', 'donepezil', 'memantine'
        ],
        'Sedative/Anesthetic': [
            'midazolam', 'diazepam', 'clonazepam', 'propofol', 'isoflurane', 'rocuronium'
        ],
        'Vitamins/Supplements': [
            'ferrous sulfate', 'vitamin b 12', 'sodium chloride'
        ],
        'Corticosteroid': [
            'hydrocortisone'
        ],
        'Diuretic': [
            'furosemide'
        ],
        'Anticonvulsant': [
            'carbamazepine'
        ],
        'Antiemetic': [
            'ondansetron'
        ]
    }
    
    for drug_class, keywords in class_dict.items():
        for keyword in keywords:
            if keyword.lower() in text:
                return drug_class
                
    return 'General/Unknown'
    
    for drug_class, keywords in class_dict.items():
        for keyword in keywords:
            if keyword in text:
                return drug_class
                
    return 'General/Unknown'

def main():
    print("Bắt đầu quá trình ETL Data Warehouse...\n")

    # 1. EXTRACT (ĐỌC 3 FILE DỮ LIỆU ĐÃ LÀM SẠCH)

    payers_df = pd.read_csv('payers_cleaned.csv')
    meds_df = pd.read_csv('medications_cleaned.csv')
    encounters_df = pd.read_csv('encounters_cleaned.csv')

    # 2. TRANSFORM CÁC BẢNG DIMENSION
    # Bảng Dim_Payer
    print("Đang xây dựng bảng Dim_Payer...")
    dim_payer = payers_df[['Id', 'NAME', 'STATE_HEADQUARTERED']].copy()
    dim_payer.rename(columns={
        'NAME': 'Name',
        'STATE_HEADQUARTERED': 'State_Headquartered'
    }, inplace=True)
    
    dim_payer['State_Headquartered'] = dim_payer['State_Headquartered'].fillna('Unknown')
    dim_payer = dim_payer.drop_duplicates(subset=['Id'])

    # Bảng Dim_Medication
    print("Đang xây dựng bảng Dim_Medication và bóc tách thông tin thuốc...")
    dim_medication = meds_df[['CODE', 'DESCRIPTION']].drop_duplicates().copy()
    dim_medication.rename(columns={
        'CODE': 'Code',
        'DESCRIPTION': 'Name'
    }, inplace=True)
    
    # Áp dụng 3 hàm chuẩn hóa riêng biệt
    dim_medication['Form'] = dim_medication['Name'].apply(extract_form)
    dim_medication['Strength'] = dim_medication['Name'].apply(extract_strength)
    dim_medication['Drug_Class'] = dim_medication['Name'].apply(predict_drug_class)
    
    # Giữ lại Code duy nhất làm Primary Key
    dim_medication = dim_medication.drop_duplicates(subset=['Code'], keep='first')

    # 3. TRANSFORM BẢNG FACT
    print("Đang tổng hợp bảng Fact_Medications...")
    
    # Kéo Provider_Id từ bảng Encounters sang
    fact_meds = pd.merge(
        meds_df, 
        encounters_df[['Id', 'PROVIDER']], 
        left_on='ENCOUNTER', 
        right_on='Id', 
        how='left'
    )

    # Sinh ID ngẫu nhiên cho bảng Fact
    fact_meds['Id_Fact'] = [str(uuid.uuid4()) for _ in range(len(fact_meds))]

    # Map các cột 
    fact_meds.rename(columns={
        'Id_Fact': 'Id',
        'ENCOUNTER': 'Encounter_Id',
        'CODE': 'Medication_Code',
        'PATIENT': 'Patient_Id',
        'PROVIDER': 'Provider_Id',
        'PAYER': 'Payer_Id',
        'BASE_COST': 'Base_Cost',
        'PAYER_COVERAGE': 'Covered_Cost'
    }, inplace=True)

    # Transform Date -> Date Keys (YYYYMMDD)
    fact_meds['START'] = pd.to_datetime(fact_meds['START'], utc=True)
    fact_meds['STOP'] = pd.to_datetime(fact_meds['STOP'], utc=True)

    fact_meds['Start_Date_Key'] = fact_meds['START'].dt.strftime('%Y%m%d').astype(int)
    fact_meds['End_Date_Key'] = fact_meds['STOP'].dt.strftime('%Y%m%d').fillna(-1).astype(int)

    # Tính toán ngày uống (Duration)
    fact_meds['Duration_Days'] = (fact_meds['STOP'] - fact_meds['START']).dt.days
    fact_meds['Duration_Days'] = fact_meds['Duration_Days'].fillna(0).astype(int)

    fact_meds['Dosage'] = 1.0
    fact_meds['Frequency'] = 'Daily'

    # Sắp xếp đúng trình tự cột 
    final_fact_columns = [
        'Id', 'Encounter_Id', 'Medication_Code', 'Patient_Id', 'Provider_Id', 
        'Payer_Id', 'Start_Date_Key', 'End_Date_Key', 'Dosage', 'Frequency', 
        'Duration_Days', 'Base_Cost', 'Covered_Cost'
    ]
    fact_medications = fact_meds[final_fact_columns]
    # 4. LOAD (XUẤT RA DỮ LIỆU ĐÍCH)
    print("Đang lưu các bảng ra file CSV Data Warehouse...")
    dim_payer.to_csv('Dim_Payer.csv', index=False)
    dim_medication.to_csv('Dim_Medication.csv', index=False)
    fact_medications.to_csv('Fact_Medications.csv', index=False)

if __name__ == "__main__":
    main()