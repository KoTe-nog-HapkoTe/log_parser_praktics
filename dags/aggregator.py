import pandas as pd

def aggregar(parsed_csv: str = "parsed_logs.csv", output_csv: str = "aggregated_logs.csv"):
    df = pd.read_csv(parsed_csv, parse_dates=["datetime"])
    required_columns = {"date", "hour", "user_name"}
    if not required_columns.issubset(df.columns):
        raise ValueError("В файле отсутствуют необходимые столбцы.")

    agg_df = df.groupby(["date", "hour", "user_name"]).size().reset_index(name="request_count")
    agg_df.to_csv(output_csv, index=False, encoding="utf-8")
    print(f"[aggregator] Aggregated {len(agg_df)} rows.")
    return agg_df

def build_hourly_calendar(df: pd.DataFrame) -> pd.DataFrame:
    # Убедимся, что нужные колонки есть
    required = {"date", "hour", "user_name"}
    if not required.issubset(df.columns):
        raise ValueError("Недостаточно данных для hour_calendar.")

    # Создаём структуру вида: (date, user_name) -> set(hours)
    user_hour_map = df.groupby(["date", "user_name"])["hour"].apply(set).reset_index()

    # Преобразуем в массив [True, False, ..., True] длиной 24
    def to_calendar(hour_set):
        return [h in hour_set for h in range(24)]

    user_hour_map["hour_calendar"] = user_hour_map["hour"].apply(to_calendar)
    user_hour_map.drop(columns=["hour"], inplace=True)
    return user_hour_map

