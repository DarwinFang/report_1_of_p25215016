import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta



# --- 設定 ---
input_xml = 'sleep_data_only.xml'
output_csv = 'final_sleep_analysis.csv'

def parse_sleep_data(xml_file):
    print("データを処理中：断片化された記録を日別統計に統合しています...")
    
    # 解析されたすべての断片を格納するリスト
    records = []
    
    context = ET.iterparse(xml_file, events=("end",))
    for event, elem in context:
        if elem.tag == 'Record':
            # 1. 数値カテゴリの抽出（Deep, Core, REM, Awake）
            raw_value = elem.get('value')
            # 名称を簡略化：AsleepDeep, AsleepCore などの接尾辞のみを保持
            sleep_stage = raw_value.replace('HKCategoryValueSleepAnalysis', '')
            
            # 2. 時刻情報の抽出と変換
            # Apple の時刻形式は通常 "2023-04-17 01:32:00 +0900"
            # 先頭19文字 "2023-04-17 01:32:00" のみを使用し，タイムゾーンは無視して計算する
            try:
                start_str = elem.get('startDate')[:19]
                end_str = elem.get('endDate')[:19]
                
                t_start = datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S')
                t_end = datetime.strptime(end_str, '%Y-%m-%d %H:%M:%S')
                
                # この断片の継続時間（分）を計算
                duration_minutes = (t_end - t_start).total_seconds() / 60.0
                
                # 3. この睡眠が属する「日付」を決定
                # ロジック：覚醒時刻（End）が4月17日朝であれば，4月17日の記録とする
                date_label = t_end.date() 
                
                records.append({
                    'Date': date_label,
                    'Stage': sleep_stage,
                    'Duration': duration_minutes,
                    'StartTime': t_start
                })
                
            except Exception as e:
                pass  # フォーマット不正の行はスキップ
            
            elem.clear()
            
    # DataFrame に変換して処理を容易にする
    df = pd.DataFrame(records)
    
    if df.empty:
        print("データを抽出できませんでした。XML ファイルが空でないか確認してください。")
        return

    # --- 集約：日付ごとに集計 ---
    print(f"元の断片数は {len(df)} 件です。集約処理を開始します...")

    # 1. 各睡眠段階の総時間を計算
    pivot_df = df.pivot_table(
        index='Date', 
        columns='Stage', 
        values='Duration', 
        aggfunc='sum',
        fill_value=0  # 該当する睡眠段階が存在しない日は 0 で補完
    )
    
    # 2. 当日の「入眠時刻」を算出（最も早い開始時刻）
    # 難点：時刻を数値に変換する必要がある（例：23:30 → -0.5，01:00 → 1.0）
    # ここでは簡略化し，当夜で最も早い記録の時刻を用いる
    
    def get_bedtime_hour(group):
        # 当日の最も早い開始時刻を取得
        min_time = group['StartTime'].min()
        
        # 時刻を「深夜0時からのオフセット」として表現
        # 00:30 → 0.5，23:30 → -0.5
        
        hour = min_time.hour
        minute = min_time.minute
        
        # 簡易的なルール：18時以降は 24 を減算
        # 例：23:00 → -1.0，01:30 → 1.5
        time_value = hour + minute / 60.0
        if hour > 18: 
            time_value = time_value - 24.0
            
        return time_value

    bedtime_series = df.groupby('Date').apply(get_bedtime_hour)
    bedtime_series.name = 'BedTime_Offset'
    
    # 結果を結合
    final_df = pivot_df.join(bedtime_series)
    
    # 列名を見やすく整形
    final_df.columns = [c.replace('Asleep', '') for c in final_df.columns]
    
    # 3. 総睡眠時間を計算
    # Core + Deep + REM を総睡眠時間と定義
    cols_to_sum = [c for c in final_df.columns if c in ['Core', 'Deep', 'REM', 'Unspecified']]
    final_df['TotalSleep'] = final_df[cols_to_sum].sum(axis=1)

    # 保存処理
    final_df.to_csv(output_csv)
    print("-" * 30)
    print(f"完了しました。分析結果ファイルを生成しました: {output_csv}")
    print(f"{len(final_df)} 日分のデータが含まれています。")
    print(final_df.head())  # 先頭数行を表示して確認
    print("-" * 30)

# --- 実行 ---
if __name__ == "__main__":
    parse_sleep_data(input_xml)
