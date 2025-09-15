import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from obis_api_call import get_obis_yearly_records

def predict_overall(species_list, start_year=None, end_year=None, future_years=5):
    species_data = {}
    all_years = set()

    # 1. Collect data for each species
    for sp in species_list:
        df = get_obis_yearly_records(sp, start_year=1900, end_year=2100)
        if df is None or df.empty:
            continue

        # Apply year filters
        if start_year:
            df = df[df["Year"] >= start_year]
        if end_year:
            df = df[df["Year"] <= end_year]

        if not df.empty:
            df = df.sort_values("Year")
            species_data[sp] = dict(zip(df["Year"], df["OBIS_Records"]))
            all_years.update(df["Year"].tolist())

    if not species_data:
        return {"error": "No valid data found for given species"}

    all_years = sorted(all_years)

    # 2. Build total count per year
    totals = []
    breakdown = {}
    for year in all_years:
        year_total = 0
        year_breakdown = {}
        for sp, records in species_data.items():
            count = records.get(year, 0)
            year_total += count
            year_breakdown[sp] = count
        totals.append(year_total)
        breakdown[year] = year_breakdown

    df_total = pd.DataFrame({"Year": all_years, "Total": totals})

    # 3. Forecast future totals with Linear Regression
    X = df_total["Year"].values.reshape(-1, 1)
    y = df_total["Total"].values
    model = LinearRegression()
    model.fit(X, y)

    future_years_arr = np.array(range(df_total["Year"].max() + 1,
                                      df_total["Year"].max() + 1 + future_years)).reshape(-1, 1)
    predictions = model.predict(future_years_arr)

    # Clamp negatives to zero
    predictions = [max(0, int(p)) for p in predictions]

    # Extend breakdown with empty species values for predicted years
    for y in future_years_arr.flatten():
        breakdown[int(y)] = {sp: None for sp in species_data.keys()}

    # 4. Build JSON result
    result = {
        "overall": {
            "year": df_total["Year"].tolist() + future_years_arr.flatten().tolist(),
            "records": df_total["Total"].tolist() + predictions,
            "type": ["historical"] * len(df_total) + ["predicted"] * future_years
        },
        "breakdown": breakdown
    }
    return result

if __name__ == "__main__":
    species_list = ["Thunnus albacares", "Larus fuscus"]
    res = predict_overall(species_list, start_year=2000, end_year=2020, future_years=5)
    print(res)

