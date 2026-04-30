# Electricity Price Forecasting — Spanish Spot Market (OMIE)

Prediction of hourly electricity prices in the Spanish wholesale electricity 
market using 6 years of historical data (2019–2024).

## Project Structure
- `notebooks/` — EDA, feature engineering, modeling and results
- `src/` — scraping and data collection functions
- `data/` — raw and processed data (not tracked in git)
- `models/` — trained models (not tracked in git)
- `outputs/` — figures and visualizations

## Methods
- Data: OMIE hourly spot prices (2019–2024) via official API
- Models: Ridge Regression, Random Forest, XGBoost
- Best result: XGBoost MAE = 5.86 €/MWh on 2024 test set

## Key Findings
- Lag_1 (previous hour price) is the dominant predictor
- Two distinct market regimes identified: pre and post Iberian Exception (June 2022)
- Duck curve clearly emerged in 2023, driven by solar capacity growth

## Next Steps
- Integrate ESIOS renewable generation data (solar, wind, hydro)
- Add Open-Meteo weather features
- Deploy interactive dashboard with Streamlit

## Requirements
pip install -r requirements.txt